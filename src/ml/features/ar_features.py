"""
AR Feature Engineering for ML Models

Creates features from invoice and payment history to predict:
- Payment propensity (will they pay?)
- Days to payment (when will they pay?)
- Collection priority scoring

Author: Vignesh
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, Dict
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def load_data(dbt_project_path: Path = None) -> Dict[str, pd.DataFrame]:
    """Load data from dbt seeds"""
    if dbt_project_path is None:
        dbt_project_path = PROJECT_ROOT / "dbt_project"
    
    seeds_path = dbt_project_path / "seeds"
    
    data = {
        'customers': pd.read_csv(seeds_path / "cusmas.csv"),
        'invoices': pd.read_csv(seeds_path / "armas.csv"),
        'payments': pd.read_csv(seeds_path / "paytran.csv"),
    }
    
    # Parse dates
    date_columns = {
        'customers': ['created_date', 'updated_date'],
        'invoices': ['invoice_date', 'due_date', 'ship_date', 'gl_post_date', 'created_date', 'updated_date'],
        'payments': ['payment_date', 'applied_date', 'created_date', 'updated_date']
    }
    
    for table, columns in date_columns.items():
        for col in columns:
            if col in data[table].columns:
                data[table][col] = pd.to_datetime(data[table][col], errors='coerce')
    
    return data


def create_customer_features(customers: pd.DataFrame, invoices: pd.DataFrame, 
                             payments: pd.DataFrame) -> pd.DataFrame:
    """
    Create customer-level features for ML models.
    
    Features include:
    - Customer demographics (segment, region, credit info)
    - Historical payment behavior
    - Invoice patterns
    """
    print("Creating customer features...")
    
    # Start with customer base
    features = customers[['customer_id', 'segment', 'region', 'industry_code',
                          'credit_limit', 'payment_terms', 'credit_status', 
                          'account_status', 'created_date']].copy()
    
    # === Invoice History Features ===
    invoice_agg = invoices.groupby('customer_id').agg({
        'invoice_number': 'count',
        'invoice_amount': ['sum', 'mean', 'std', 'min', 'max'],
        'current_balance': 'sum',
        'invoice_date': ['min', 'max'],
    }).reset_index()
    
    # Flatten column names
    invoice_agg.columns = ['customer_id', 'total_invoices', 'total_invoice_amount',
                           'avg_invoice_amount', 'std_invoice_amount', 
                           'min_invoice_amount', 'max_invoice_amount',
                           'total_ar_balance', 'first_invoice_date', 'last_invoice_date']
    
    features = features.merge(invoice_agg, on='customer_id', how='left')
    
    # === Invoice Status Features ===
    status_counts = invoices.groupby(['customer_id', 'status']).size().unstack(fill_value=0)
    status_counts.columns = [f'invoice_status_{col}' for col in status_counts.columns]
    status_counts = status_counts.reset_index()
    
    features = features.merge(status_counts, on='customer_id', how='left')
    
    # === Payment History Features ===
    payment_agg = payments.groupby('customer_id').agg({
        'payment_id': 'count',
        'payment_amount': ['sum', 'mean'],
        'payment_date': ['min', 'max'],
    }).reset_index()
    
    payment_agg.columns = ['customer_id', 'total_payments', 'total_payment_amount',
                           'avg_payment_amount', 'first_payment_date', 'last_payment_date']
    
    features = features.merge(payment_agg, on='customer_id', how='left')
    
    # === Payment Method Features ===
    method_counts = payments.groupby(['customer_id', 'payment_method']).size().unstack(fill_value=0)
    method_counts.columns = [f'payment_method_{col}' for col in method_counts.columns]
    method_counts = method_counts.reset_index()
    
    features = features.merge(method_counts, on='customer_id', how='left')
    
    # === Derived Features ===
    # Payment rate
    features['payment_rate'] = (
        features['total_payment_amount'] / 
        features['total_invoice_amount'].replace(0, np.nan)
    ).fillna(0)
    
    # Average AR balance per invoice
    features['avg_ar_per_invoice'] = (
        features['total_ar_balance'] / 
        features['total_invoices'].replace(0, np.nan)
    ).fillna(0)
    
    # Customer tenure (days since first invoice)
    reference_date = pd.Timestamp('2024-12-31')
    features['customer_tenure_days'] = (
        reference_date - features['first_invoice_date']
    ).dt.days.fillna(0)
    
    # Days since last payment
    features['days_since_last_payment'] = (
        reference_date - features['last_payment_date']
    ).dt.days.fillna(9999)
    
    # Credit utilization
    features['credit_utilization'] = (
        features['total_ar_balance'] / 
        features['credit_limit'].replace(0, np.nan)
    ).fillna(0).clip(0, 2)  # Cap at 200%
    
    # Fill NaN values
    features = features.fillna(0)
    
    print(f"  Created {len(features.columns)} features for {len(features)} customers")
    
    return features


def create_invoice_features(invoices: pd.DataFrame, customers: pd.DataFrame,
                            payments: pd.DataFrame) -> pd.DataFrame:
    """
    Create invoice-level features for payment prediction.
    
    Each row represents one invoice with features to predict:
    - Will this invoice be paid? (classification)
    - How many days until payment? (regression)
    """
    print("Creating invoice features...")
    
    # Start with invoice base
    features = invoices[['invoice_number', 'customer_id', 'invoice_date', 'due_date',
                         'invoice_amount', 'tax_amount', 'current_balance', 
                         'status', 'payment_terms', 'dispute_flag']].copy()
    
    # === Invoice-level features ===
    reference_date = pd.Timestamp('2024-12-31')
    
    # Days since invoice
    features['days_since_invoice'] = (reference_date - features['invoice_date']).dt.days
    
    # Days past due
    features['days_past_due'] = (reference_date - features['due_date']).dt.days
    features['days_past_due'] = features['days_past_due'].clip(lower=0)
    
    # Invoice amount features
    features['total_amount'] = features['invoice_amount'] + features['tax_amount']
    features['is_large_invoice'] = (features['total_amount'] > features['total_amount'].quantile(0.75)).astype(int)
    
    # Is disputed
    features['is_disputed'] = (features['dispute_flag'] == 'Y').astype(int)
    
    # === Add customer features ===
    customer_features = customers[['customer_id', 'segment', 'region', 'credit_limit',
                                   'credit_status', 'account_status']].copy()
    
    features = features.merge(customer_features, on='customer_id', how='left')
    
    # === Historical payment behavior for this customer ===
    # Get payment history before each invoice
    customer_history = payments.groupby('customer_id').agg({
        'payment_id': 'count',
        'payment_amount': 'sum',
        'applied_flag': lambda x: (x == 'Y').mean()  # Application rate
    }).reset_index()
    
    customer_history.columns = ['customer_id', 'customer_payment_count', 
                                'customer_total_paid', 'customer_application_rate']
    
    features = features.merge(customer_history, on='customer_id', how='left')
    
    # === Target Variable ===
    # Was this invoice paid? (for classification)
    features['is_paid'] = features['status'].isin(['PD', 'PP']).astype(int)
    
    # === Encode categorical variables ===
    # Segment
    segment_map = {'E': 4, 'M': 3, 'S': 2, 'T': 1}
    features['segment_encoded'] = features['segment'].map(segment_map).fillna(0)
    
    # Region
    features = pd.get_dummies(features, columns=['region'], prefix='region', dummy_na=False)
    
    # Credit status
    credit_map = {'A': 2, 'H': 1, 'S': 0}
    features['credit_status_encoded'] = features['credit_status'].map(credit_map).fillna(0)
    
    # Fill NaN
    features = features.fillna(0)
    
    print(f"  Created {len(features.columns)} features for {len(features)} invoices")
    
    return features


def prepare_training_data(features: pd.DataFrame, 
                          target_col: str = 'is_paid') -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepare features and target for model training.
    
    Returns:
        X: Feature matrix
        y: Target variable
    """
    # Columns to exclude from features
    exclude_cols = [
        'invoice_number', 'customer_id', 'invoice_date', 'due_date',
        'status', 'dispute_flag', 'segment', 'credit_status', 'account_status',
        'is_paid', 'current_balance'  # Target and leakage
    ]
    
    # Get feature columns
    feature_cols = [col for col in features.columns if col not in exclude_cols]
    
    X = features[feature_cols].copy()
    y = features[target_col].copy()
    
    print(f"  Features: {X.shape[1]}, Samples: {X.shape[0]}")
    print(f"  Target distribution: {y.value_counts().to_dict()}")
    
    return X, y


def create_collection_priority_features(invoices: pd.DataFrame, 
                                         customers: pd.DataFrame) -> pd.DataFrame:
    """
    Create features for collection priority scoring.
    
    Used to rank which accounts AR team should focus on.
    """
    print("Creating collection priority features...")
    
    # Get open invoices only
    open_invoices = invoices[invoices['status'].isin(['OP', 'PP', 'DP'])].copy()
    
    if len(open_invoices) == 0:
        print("  No open invoices found!")
        return pd.DataFrame()
    
    # Reference date
    reference_date = pd.Timestamp('2024-12-31')
    
    # Calculate aging
    open_invoices['days_past_due'] = (reference_date - pd.to_datetime(open_invoices['due_date'])).dt.days
    open_invoices['days_past_due'] = open_invoices['days_past_due'].clip(lower=0)
    
    # Add customer info
    open_invoices = open_invoices.merge(
        customers[['customer_id', 'customer_name', 'segment', 'credit_limit', 'payment_terms']],
        on='customer_id',
        how='left'
    )
    
    # Aggregate to customer level
    collection_df = open_invoices.groupby('customer_id').agg({
        'customer_name': 'first',
        'segment': 'first',
        'credit_limit': 'first',
        'invoice_number': 'count',
        'current_balance': 'sum',
        'days_past_due': 'max',
    }).reset_index()
    
    collection_df.columns = ['customer_id', 'customer_name', 'segment', 'credit_limit',
                             'open_invoice_count', 'total_ar_balance', 'max_days_past_due']
    
    # === Calculate Priority Score ===
    # Higher score = higher priority for collection
    
    # Amount factor (0-40 points) - higher balance = higher priority
    balance_percentile = collection_df['total_ar_balance'].rank(pct=True)
    collection_df['amount_score'] = (balance_percentile * 40).round(0)
    
    # Aging factor (0-40 points) - older = higher priority
    collection_df['aging_score'] = np.minimum(collection_df['max_days_past_due'] / 90 * 40, 40).round(0)
    
    # Segment factor (0-20 points) - enterprise = higher priority
    segment_scores = {'E': 20, 'M': 15, 'S': 10, 'T': 5}
    collection_df['segment_score'] = collection_df['segment'].map(segment_scores).fillna(5)
    
    # Total priority score
    collection_df['priority_score'] = (
        collection_df['amount_score'] + 
        collection_df['aging_score'] + 
        collection_df['segment_score']
    )
    
    # Priority tier
    collection_df['priority_tier'] = pd.cut(
        collection_df['priority_score'],
        bins=[0, 33, 66, 100],
        labels=['Low', 'Medium', 'High']
    )
    
    # Sort by priority
    collection_df = collection_df.sort_values('priority_score', ascending=False)
    
    print(f"  Created priority scores for {len(collection_df)} customers with open AR")
    print(f"  Priority distribution: {collection_df['priority_tier'].value_counts().to_dict()}")
    
    return collection_df


if __name__ == "__main__":
    print("="*60)
    print("AR Feature Engineering")
    print("="*60)
    
    # Load data
    print("\nLoading data...")
    data = load_data()
    print(f"  Customers: {len(data['customers'])}")
    print(f"  Invoices: {len(data['invoices'])}")
    print(f"  Payments: {len(data['payments'])}")
    
    # Create features
    print("\n" + "-"*60)
    customer_features = create_customer_features(
        data['customers'], data['invoices'], data['payments']
    )
    
    print("\n" + "-"*60)
    invoice_features = create_invoice_features(
        data['invoices'], data['customers'], data['payments']
    )
    
    print("\n" + "-"*60)
    X, y = prepare_training_data(invoice_features)
    
    print("\n" + "-"*60)
    collection_priority = create_collection_priority_features(
        data['invoices'], data['customers']
    )
    
    print("\n" + "="*60)
    print("Feature engineering complete!")
    print("="*60)
    
    # Show sample
    print("\nSample collection priorities:")
    if len(collection_priority) > 0:
        print(collection_priority[['customer_name', 'total_ar_balance', 'max_days_past_due', 
                                   'priority_score', 'priority_tier']].head(10).to_string())