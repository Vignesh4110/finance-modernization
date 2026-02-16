"""
Collection Priority Scorer

Combines ML predictions with business rules to prioritize collection efforts.

Priority Score = (Payment Probability * Amount Weight) + Aging Weight + Segment Weight

This helps AR teams focus on:
1. Large balances that are likely to be recovered
2. Accounts that are getting dangerously old
3. Strategic customers (Enterprise segment)

Author: Vignesh
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ml.features.ar_features import load_data, create_collection_priority_features
from src.ml.models.payment_propensity import PaymentPropensityModel, score_invoices

MODELS_DIR = PROJECT_ROOT / "src" / "ml" / "trained_models"


class CollectionPriorityScorer:
    """
    Scores and ranks accounts for collection priority.
    
    Combines:
    - ML payment probability predictions
    - Invoice aging
    - Balance amounts
    - Customer segment importance
    """
    
    def __init__(self, weights: Dict[str, float] = None):
        """
        Initialize scorer with custom weights.
        
        Args:
            weights: Dictionary of scoring weights
                - payment_prob: Weight for payment probability (default 0.3)
                - amount: Weight for balance amount (default 0.3)
                - aging: Weight for days past due (default 0.25)
                - segment: Weight for customer segment (default 0.15)
        """
        default_weights = {
            'payment_prob': 0.30,
            'amount': 0.30,
            'aging': 0.25,
            'segment': 0.15
        }
        
        self.weights = {**default_weights, **(weights or {})}
        self.payment_model = None
    
    def load_payment_model(self) -> None:
        """Load the trained payment propensity model"""
        model_dirs = sorted(MODELS_DIR.glob("payment_propensity_*"))
        if model_dirs:
            self.payment_model = PaymentPropensityModel.load(model_dirs[-1])
            print(f"Loaded payment model from: {model_dirs[-1]}")
        else:
            print("Warning: No payment model found. Using rule-based scoring only.")
    
    def score_accounts(self, include_ml: bool = True) -> pd.DataFrame:
        """
        Score all accounts with open AR.
        
        Args:
            include_ml: Whether to include ML payment probability
            
        Returns:
            DataFrame with collection priority scores
        """
        print("="*60)
        print("Collection Priority Scoring")
        print("="*60)
        
        # Load data
        data = load_data()
        invoices = data['invoices']
        customers = data['customers']
        
        # Get open invoices
        open_invoices = invoices[invoices['status'].isin(['OP', 'PP', 'DP'])].copy()
        
        if len(open_invoices) == 0:
            print("No open invoices found!")
            return pd.DataFrame()
        
        print(f"\nOpen invoices: {len(open_invoices)}")
        
        # Parse dates
        open_invoices['due_date'] = pd.to_datetime(open_invoices['due_date'])
        reference_date = pd.Timestamp('2024-12-31')
        
        # Calculate days past due
        open_invoices['days_past_due'] = (reference_date - open_invoices['due_date']).dt.days
        open_invoices['days_past_due'] = open_invoices['days_past_due'].clip(lower=0)
        
        # Aggregate to customer level
        customer_ar = open_invoices.groupby('customer_id').agg({
            'invoice_number': 'count',
            'current_balance': 'sum',
            'invoice_amount': 'sum',
            'days_past_due': ['max', 'mean'],
            'status': lambda x: (x == 'DP').sum()  # Count disputed
        }).reset_index()
        
        customer_ar.columns = ['customer_id', 'open_invoice_count', 'total_ar_balance',
                               'total_invoice_amount', 'max_days_past_due', 
                               'avg_days_past_due', 'disputed_count']
        
        # Add customer details
        customer_ar = customer_ar.merge(
            customers[['customer_id', 'customer_name', 'segment', 'region',
                      'credit_limit', 'payment_terms', 'email']],
            on='customer_id',
            how='left'
        )
        
        # === Calculate Component Scores ===
        
        # 1. Amount Score (0-100): Higher balance = higher priority
        max_balance = customer_ar['total_ar_balance'].max()
        customer_ar['amount_score'] = (
            customer_ar['total_ar_balance'] / max_balance * 100
        ).round(1)
        
        # 2. Aging Score (0-100): More days past due = higher priority
        # Cap at 180 days for scoring
        customer_ar['aging_score'] = (
            np.minimum(customer_ar['max_days_past_due'], 180) / 180 * 100
        ).round(1)
        
        # 3. Segment Score (0-100): Enterprise > Mid > Small > Startup
        segment_scores = {'E': 100, 'M': 75, 'S': 50, 'T': 25}
        customer_ar['segment_score'] = customer_ar['segment'].map(segment_scores).fillna(25)
        
        # 4. Payment Probability Score (0-100)
        if include_ml and self.payment_model is not None:
            # Get ML scores at invoice level and aggregate
            try:
                scored_invoices = score_invoices(self.payment_model)
                
                # Merge with open invoices
                open_with_proba = open_invoices.merge(
                    scored_invoices[['invoice_number', 'payment_probability']],
                    on='invoice_number',
                    how='left'
                )
                
                # Aggregate probability by customer (weighted by amount)
                customer_proba = open_with_proba.groupby('customer_id').apply(
                    lambda x: np.average(x['payment_probability'], 
                                        weights=x['current_balance'].clip(lower=1))
                ).reset_index(name='avg_payment_prob')
                
                customer_ar = customer_ar.merge(customer_proba, on='customer_id', how='left')
                customer_ar['payment_prob_score'] = (
                    customer_ar['avg_payment_prob'] * 100
                ).fillna(50).round(1)
                
            except Exception as e:
                print(f"Warning: Could not get ML scores: {e}")
                customer_ar['payment_prob_score'] = 50  # Default
        else:
            customer_ar['payment_prob_score'] = 50  # Default without ML
        
        # === Calculate Final Priority Score ===
        customer_ar['priority_score'] = (
            customer_ar['payment_prob_score'] * self.weights['payment_prob'] +
            customer_ar['amount_score'] * self.weights['amount'] +
            customer_ar['aging_score'] * self.weights['aging'] +
            customer_ar['segment_score'] * self.weights['segment']
        ).round(1)
        
        # Priority tier
        customer_ar['priority_tier'] = pd.cut(
            customer_ar['priority_score'],
            bins=[0, 33, 66, 100],
            labels=['Low', 'Medium', 'High']
        )
        
        # Recommended action
        def get_action(row):
            if row['priority_tier'] == 'High':
                if row['max_days_past_due'] > 90:
                    return "URGENT: Escalate to management"
                elif row['disputed_count'] > 0:
                    return "Review disputes, then call"
                else:
                    return "Call immediately"
            elif row['priority_tier'] == 'Medium':
                if row['max_days_past_due'] > 60:
                    return "Send reminder + follow-up call"
                else:
                    return "Send payment reminder email"
            else:
                return "Monitor - send statement"
        
        customer_ar['recommended_action'] = customer_ar.apply(get_action, axis=1)
        
        # Sort by priority
        customer_ar = customer_ar.sort_values('priority_score', ascending=False)
        
        # === Summary Statistics ===
        print(f"\nCustomers with open AR: {len(customer_ar)}")
        print(f"Total AR Balance: ${customer_ar['total_ar_balance'].sum():,.2f}")
        print(f"\nPriority Distribution:")
        print(customer_ar['priority_tier'].value_counts().to_string())
        
        return customer_ar
    
    def get_collection_worklist(self, top_n: int = 20) -> pd.DataFrame:
        """
        Get prioritized worklist for AR team.
        
        Returns top N accounts to focus on today.
        """
        all_accounts = self.score_accounts()
        
        # Select columns for worklist
        worklist_cols = [
            'customer_name', 'segment', 'total_ar_balance', 'open_invoice_count',
            'max_days_past_due', 'priority_score', 'priority_tier', 
            'recommended_action', 'email'
        ]
        
        worklist = all_accounts[worklist_cols].head(top_n)
        
        return worklist
    
    def generate_collection_report(self) -> str:
        """Generate a formatted collection report"""
        accounts = self.score_accounts(include_ml=False)  # Faster without ML
        
        report = []
        report.append("="*70)
        report.append("COLLECTION PRIORITY REPORT")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("="*70)
        
        # Summary
        report.append(f"\nSUMMARY")
        report.append("-"*70)
        report.append(f"Total Accounts with Open AR: {len(accounts)}")
        report.append(f"Total AR Balance: ${accounts['total_ar_balance'].sum():,.2f}")
        report.append(f"High Priority Accounts: {(accounts['priority_tier'] == 'High').sum()}")
        report.append(f"Medium Priority Accounts: {(accounts['priority_tier'] == 'Medium').sum()}")
        report.append(f"Low Priority Accounts: {(accounts['priority_tier'] == 'Low').sum()}")
        
        # High priority details
        report.append(f"\nHIGH PRIORITY ACCOUNTS (Top 10)")
        report.append("-"*70)
        
        high_priority = accounts[accounts['priority_tier'] == 'High'].head(10)
        for _, row in high_priority.iterrows():
            report.append(
                f"  {row['customer_name'][:30]:<30} "
                f"${row['total_ar_balance']:>12,.2f}  "
                f"{row['max_days_past_due']:>3} days  "
                f"Score: {row['priority_score']:.0f}"
            )
        
        report.append("\n" + "="*70)
        
        return "\n".join(report)


def run_collection_scoring():
    """Run the full collection scoring pipeline"""
    scorer = CollectionPriorityScorer()
    
    # Try to load ML model
    scorer.load_payment_model()
    
    # Get worklist
    print("\n" + "-"*60)
    print("Top 20 Collection Priorities")
    print("-"*60)
    
    worklist = scorer.get_collection_worklist(top_n=20)
    
    # Format for display
    display_cols = ['customer_name', 'total_ar_balance', 'max_days_past_due', 
                    'priority_score', 'priority_tier', 'recommended_action']
    
    print(worklist[display_cols].to_string(index=False))
    
    # Generate report
    print("\n")
    report = scorer.generate_collection_report()
    print(report)
    
    return worklist


if __name__ == "__main__":
    worklist = run_collection_scoring()