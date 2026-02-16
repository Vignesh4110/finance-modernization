"""
Payment Propensity Model

Predicts the likelihood that an invoice will be paid.
Used to prioritize collection efforts on accounts most likely to pay.

Model: XGBoost Classifier
Target: is_paid (0/1)
Features: Invoice characteristics, customer history, payment patterns

Author: Vignesh
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import joblib
import json
from typing import Dict, Tuple, Any
import sys

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, classification_report, confusion_matrix
)
from xgboost import XGBClassifier

# Add project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ml.features.ar_features import load_data, create_invoice_features, prepare_training_data

# Model output directory
MODELS_DIR = PROJECT_ROOT / "src" / "ml" / "trained_models"


class PaymentPropensityModel:
    """
    Predicts probability that an invoice will be paid.
    
    Usage:
        model = PaymentPropensityModel()
        model.train(X_train, y_train)
        predictions = model.predict_proba(X_test)
    """
    
    def __init__(self, model_params: Dict = None):
        """Initialize model with parameters"""
        
        default_params = {
            'n_estimators': 100,
            'max_depth': 5,
            'learning_rate': 0.1,
            'min_child_weight': 3,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'objective': 'binary:logistic',
            'random_state': 42,
            'n_jobs': -1,
            'eval_metric': 'auc'
        }
        
        self.params = {**default_params, **(model_params or {})}
        self.model = XGBClassifier(**self.params)
        self.scaler = StandardScaler()
        self.feature_names = None
        self.is_trained = False
        self.metrics = {}
    
    def train(self, X: pd.DataFrame, y: pd.Series, 
              validation_split: float = 0.2) -> Dict[str, float]:
        """
        Train the model.
        
        Args:
            X: Feature matrix
            y: Target variable (0/1)
            validation_split: Fraction for validation
            
        Returns:
            Dictionary of evaluation metrics
        """
        print("Training Payment Propensity Model...")
        
        # Store feature names
        self.feature_names = list(X.columns)
        
        # Split data
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=validation_split, random_state=42, stratify=y
        )
        
        print(f"  Training samples: {len(X_train)}")
        print(f"  Validation samples: {len(X_val)}")
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_val_scaled = self.scaler.transform(X_val)
        
        # Train model
        self.model.fit(
            X_train_scaled, y_train,
            eval_set=[(X_val_scaled, y_val)],
            verbose=False
        )
        
        self.is_trained = True
        
        # Evaluate
        y_pred = self.model.predict(X_val_scaled)
        y_proba = self.model.predict_proba(X_val_scaled)[:, 1]
        
        self.metrics = {
            'accuracy': accuracy_score(y_val, y_pred),
            'precision': precision_score(y_val, y_pred),
            'recall': recall_score(y_val, y_pred),
            'f1': f1_score(y_val, y_pred),
            'roc_auc': roc_auc_score(y_val, y_proba),
            'training_samples': len(X_train),
            'validation_samples': len(X_val),
            'n_features': len(self.feature_names)
        }
        
        print(f"\n  Model Performance:")
        print(f"    Accuracy:  {self.metrics['accuracy']:.3f}")
        print(f"    Precision: {self.metrics['precision']:.3f}")
        print(f"    Recall:    {self.metrics['recall']:.3f}")
        print(f"    F1 Score:  {self.metrics['f1']:.3f}")
        print(f"    ROC AUC:   {self.metrics['roc_auc']:.3f}")
        
        return self.metrics
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Predict binary outcome (paid/not paid)"""
        if not self.is_trained:
            raise ValueError("Model not trained. Call train() first.")
        
        X_scaled = self.scaler.transform(X[self.feature_names])
        return self.model.predict(X_scaled)
    
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Predict probability of payment"""
        if not self.is_trained:
            raise ValueError("Model not trained. Call train() first.")
        
        X_scaled = self.scaler.transform(X[self.feature_names])
        return self.model.predict_proba(X_scaled)[:, 1]
    
    def get_feature_importance(self, top_n: int = 15) -> pd.DataFrame:
        """Get feature importance ranking"""
        if not self.is_trained:
            raise ValueError("Model not trained. Call train() first.")
        
        importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        return importance.head(top_n)
    
    def save(self, path: Path = None) -> Path:
        """Save model to disk"""
        if not self.is_trained:
            raise ValueError("Model not trained. Call train() first.")
        
        if path is None:
            MODELS_DIR.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = MODELS_DIR / f"payment_propensity_{timestamp}"
        
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        
        # Save model
        joblib.dump(self.model, path / "model.joblib")
        
        # Save scaler
        joblib.dump(self.scaler, path / "scaler.joblib")
        
        # Save metadata
        metadata = {
            'feature_names': self.feature_names,
            'metrics': self.metrics,
            'params': self.params,
            'trained_at': datetime.now().isoformat()
        }
        with open(path / "metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"  Model saved to: {path}")
        return path
    
    @classmethod
    def load(cls, path: Path) -> 'PaymentPropensityModel':
        """Load model from disk"""
        path = Path(path)
        
        instance = cls()
        instance.model = joblib.load(path / "model.joblib")
        instance.scaler = joblib.load(path / "scaler.joblib")
        
        with open(path / "metadata.json", 'r') as f:
            metadata = json.load(f)
        
        instance.feature_names = metadata['feature_names']
        instance.metrics = metadata['metrics']
        instance.params = metadata['params']
        instance.is_trained = True
        
        return instance


def train_payment_propensity_model() -> Tuple[PaymentPropensityModel, pd.DataFrame]:
    """
    End-to-end training pipeline for payment propensity model.
    
    Returns:
        Trained model and feature importance DataFrame
    """
    print("="*60)
    print("Payment Propensity Model Training Pipeline")
    print("="*60)
    
    # Load data
    print("\n1. Loading data...")
    data = load_data()
    
    # Create features
    print("\n2. Creating features...")
    invoice_features = create_invoice_features(
        data['invoices'], data['customers'], data['payments']
    )
    
    # Prepare training data
    print("\n3. Preparing training data...")
    X, y = prepare_training_data(invoice_features, target_col='is_paid')
    
    # Train model
    print("\n4. Training model...")
    model = PaymentPropensityModel()
    metrics = model.train(X, y)
    
    # Feature importance
    print("\n5. Feature Importance (Top 15):")
    importance = model.get_feature_importance(top_n=15)
    print(importance.to_string(index=False))
    
    # Save model
    print("\n6. Saving model...")
    model.save()
    
    print("\n" + "="*60)
    print("Training complete!")
    print("="*60)
    
    return model, importance


def score_invoices(model: PaymentPropensityModel = None) -> pd.DataFrame:
    """
    Score all invoices with payment probability.
    
    Returns DataFrame with invoice details and payment probability.
    """
    print("\nScoring invoices...")
    
    # Load data
    data = load_data()
    
    # Create features
    invoice_features = create_invoice_features(
        data['invoices'], data['customers'], data['payments']
    )
    
    # Get feature columns
    if model is None:
        # Load latest model
        model_dirs = sorted(MODELS_DIR.glob("payment_propensity_*"))
        if not model_dirs:
            raise FileNotFoundError("No trained model found. Run training first.")
        model = PaymentPropensityModel.load(model_dirs[-1])
    
    # Prepare features
    X, _ = prepare_training_data(invoice_features, target_col='is_paid')
    
    # Predict
    invoice_features['payment_probability'] = model.predict_proba(X)
    
    # Add probability tier
    invoice_features['probability_tier'] = pd.cut(
        invoice_features['payment_probability'],
        bins=[0, 0.3, 0.6, 1.0],
        labels=['Low', 'Medium', 'High']
    )
    
    # Select output columns
    output_cols = ['invoice_number', 'customer_id', 'total_amount', 
                   'days_past_due', 'status', 'payment_probability', 'probability_tier']
    
    result = invoice_features[output_cols].sort_values(
        'payment_probability', ascending=False
    )
    
    print(f"  Scored {len(result)} invoices")
    print(f"  Probability tier distribution: {result['probability_tier'].value_counts().to_dict()}")
    
    return result


if __name__ == "__main__":
    # Train model
    model, importance = train_payment_propensity_model()
    
    # Score invoices
    print("\n" + "-"*60)
    scored_invoices = score_invoices(model)
    
    print("\nSample scored invoices (highest probability):")
    print(scored_invoices.head(10).to_string(index=False))
    
    print("\nSample scored invoices (lowest probability):")
    print(scored_invoices.tail(10).to_string(index=False))