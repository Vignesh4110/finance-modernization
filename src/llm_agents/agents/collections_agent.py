"""
Collections Email Agent - AI-Powered Dunning Email Generator
Uses Groq (Llama 3) for free, fast LLM inference
"""

import os
import sys
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# Try to import Groq
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    print("Warning: groq package not installed. Run: pip install groq")


class EmailTone(Enum):
    FRIENDLY = "friendly"
    FIRM = "firm"
    URGENT = "urgent"
    FINAL = "final"


@dataclass
class CustomerAccount:
    customer_id: str
    customer_name: str
    contact_name: str
    email: str
    total_balance: float
    days_past_due: int
    invoice_count: int
    payment_history_score: float  # 0-100, higher = better payer
    segment: str


@dataclass
class CollectionEmail:
    to: str
    subject: str
    body: str
    tone: EmailTone
    customer_id: str
    generated_at: datetime


class CollectionsAgent:
    """
    AI-powered collections email generator using Groq (Llama 3).
    Generates personalized dunning emails based on customer data and payment probability.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the Collections Agent."""
        self.api_key = api_key or os.getenv("GROQ_API_KEY")
        
        if GROQ_AVAILABLE and self.api_key:
            self.client = Groq(api_key=self.api_key)
            self.demo_mode = False
        else:
            self.client = None
            self.demo_mode = True
    
    def determine_tone(self, account: CustomerAccount) -> EmailTone:
        """Determine appropriate email tone based on account status."""
        if account.days_past_due <= 15:
            return EmailTone.FRIENDLY
        elif account.days_past_due <= 45:
            return EmailTone.FIRM
        elif account.days_past_due <= 75:
            return EmailTone.URGENT
        else:
            return EmailTone.FINAL
    
    def generate_email(self, account: CustomerAccount, tone: Optional[EmailTone] = None) -> CollectionEmail:
        """Generate a collection email for the given account."""
        if tone is None:
            tone = self.determine_tone(account)
        
        if self.demo_mode:
            return self._generate_template_email(account, tone)
        
        return self._generate_ai_email(account, tone)
    
    def _generate_ai_email(self, account: CustomerAccount, tone: EmailTone) -> CollectionEmail:
        """Generate email using Groq Llama 3."""
        
        tone_instructions = {
            EmailTone.FRIENDLY: "Write a warm, friendly reminder. Assume this is a simple oversight. Be helpful and maintain the relationship.",
            EmailTone.FIRM: "Write a professional but firm reminder. Express concern about the overdue status while remaining respectful.",
            EmailTone.URGENT: "Write an urgent notice. Clearly communicate the seriousness while offering to help resolve any issues.",
            EmailTone.FINAL: "Write a final notice. Be direct about consequences (collections, credit reporting) while offering one last chance to resolve."
        }
        
        prompt = f"""You are a professional accounts receivable specialist. Generate a collection email for the following customer:

CUSTOMER INFORMATION:
- Company: {account.customer_name}
- Contact: {account.contact_name}
- Outstanding Balance: ${account.total_balance:,.2f}
- Days Past Due: {account.days_past_due}
- Number of Invoices: {account.invoice_count}
- Customer Segment: {account.segment}
- Payment History Score: {account.payment_history_score}/100

TONE INSTRUCTION: {tone_instructions[tone]}

Generate a professional email with:
1. Appropriate subject line
2. Personalized greeting
3. Clear statement of the issue
4. Specific call to action
5. Professional closing

Format your response as:
SUBJECT: [subject line]
BODY:
[email body]
"""

        response = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a professional AR collections specialist who writes effective but respectful collection emails."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=500
        )
        
        # Parse response
        content = response.choices[0].message.content
        
        # Extract subject and body
        if "SUBJECT:" in content and "BODY:" in content:
            parts = content.split("BODY:")
            subject = parts[0].replace("SUBJECT:", "").strip()
            body = parts[1].strip()
        else:
            subject = f"Payment Reminder - {account.customer_name}"
            body = content
        
        return CollectionEmail(
            to=account.email,
            subject=subject,
            body=body,
            tone=tone,
            customer_id=account.customer_id,
            generated_at=datetime.now()
        )
    
    def _generate_template_email(self, account: CustomerAccount, tone: EmailTone) -> CollectionEmail:
        """Generate email using templates (demo mode)."""
        
        templates = {
            EmailTone.FRIENDLY: {
                "subject": f"Friendly Reminder: Invoice(s) Due - {account.customer_name}",
                "body": f"""Dear {account.contact_name},

I hope this message finds you well. This is a friendly reminder that you have {account.invoice_count} invoice(s) totaling ${account.total_balance:,.2f} that are approaching or past their due date.

We value your business and want to ensure your account remains in good standing. If you've already sent payment, please disregard this notice.

If you have any questions about these invoices or need to discuss payment arrangements, please don't hesitate to reach out.

Thank you for your continued partnership.

Best regards,
Accounts Receivable Team"""
            },
            EmailTone.FIRM: {
                "subject": f"Payment Required: Account Past Due - {account.customer_name}",
                "body": f"""Dear {account.contact_name},

Our records indicate that your account has an outstanding balance of ${account.total_balance:,.2f} that is now {account.days_past_due} days past due.

We understand that oversights happen, but we need to bring this matter to your immediate attention. Please arrange for payment at your earliest convenience.

Outstanding Amount: ${account.total_balance:,.2f}
Days Past Due: {account.days_past_due}
Invoice Count: {account.invoice_count}

If you're experiencing difficulties or have questions about this balance, please contact us to discuss payment options.

Sincerely,
Accounts Receivable Department"""
            },
            EmailTone.URGENT: {
                "subject": f"URGENT: Immediate Payment Required - {account.customer_name}",
                "body": f"""Dear {account.contact_name},

This is an urgent notice regarding your seriously past due account.

Your account balance of ${account.total_balance:,.2f} is now {account.days_past_due} days overdue. This situation requires your immediate attention.

If payment is not received within 10 business days, we may be forced to:
- Suspend your account and future orders
- Report the delinquency to credit agencies
- Engage collection services

We strongly encourage you to contact us TODAY to resolve this matter. We're willing to work with you on a payment plan if needed.

Please call us at (555) 123-4567 or reply to this email immediately.

Accounts Receivable Department"""
            },
            EmailTone.FINAL: {
                "subject": f"FINAL NOTICE: Account {account.customer_name} - Immediate Action Required",
                "body": f"""Dear {account.contact_name},

FINAL NOTICE

Despite our previous attempts to contact you, your account remains severely delinquent.

Outstanding Balance: ${account.total_balance:,.2f}
Days Past Due: {account.days_past_due}

This is your final notice before we escalate this matter. If payment or contact is not received within 5 business days, we will be forced to:

1. Suspend your account and all credit privileges
2. Report the delinquency to credit bureaus
3. Refer the account to our collections agency

This action will negatively impact your ability to do business with us and potentially other vendors.

To avoid these consequences, please remit payment immediately or contact us to discuss resolution options.

Final Deadline: {(datetime.now() + timedelta(days=5)).strftime('%B %d, %Y')}

Accounts Receivable Department
Phone: (555) 123-4567"""
            }
        }
        
        template = templates[tone]
        
        return CollectionEmail(
            to=account.email,
            subject=template["subject"],
            body=template["body"],
            tone=tone,
            customer_id=account.customer_id,
            generated_at=datetime.now()
        )
    
    def generate_batch_emails(self, accounts: list[CustomerAccount]) -> list[CollectionEmail]:
        """Generate emails for multiple accounts."""
        emails = []
        for account in accounts:
            email = self.generate_email(account)
            emails.append(email)
        return emails


def demo_collections_agent():
    """Demonstrate the Collections Agent."""
    print("=" * 70)
    print("COLLECTIONS EMAIL AGENT DEMO")
    print("=" * 70)
    
    agent = CollectionsAgent()
    
    if agent.demo_mode:
        print("Mode: Template (Demo) - Set GROQ_API_KEY for AI generation")
    else:
        print("Mode: AI-Powered (Groq Llama 3)")
    
    # Sample accounts with different scenarios
    test_accounts = [
        CustomerAccount(
            customer_id="C001",
            customer_name="Acme Corporation",
            contact_name="John Smith",
            email="john.smith@acme.com",
            total_balance=45000.00,
            days_past_due=15,
            invoice_count=3,
            payment_history_score=85,
            segment="Enterprise"
        ),
        CustomerAccount(
            customer_id="C002",
            customer_name="Beta Industries",
            contact_name="Sarah Johnson",
            email="sarah@beta-ind.com",
            total_balance=12500.00,
            days_past_due=45,
            invoice_count=2,
            payment_history_score=60,
            segment="Mid-Market"
        ),
        CustomerAccount(
            customer_id="C003",
            customer_name="Startup Labs",
            contact_name="Mike Chen",
            email="mike@startuplabs.io",
            total_balance=8750.00,
            days_past_due=95,
            invoice_count=5,
            payment_history_score=25,
            segment="Startup"
        ),
    ]
    
    for account in test_accounts:
        print(f"\n{'-' * 70}")
        print(f"Customer: {account.customer_name}")
        print(f"Balance: ${account.total_balance:,.2f}")
        print(f"Days Past Due: {account.days_past_due}")
        print(f"Payment Probability: {account.payment_history_score}%")
        
        email = agent.generate_email(account)
        
        print(f"\nTone: {email.tone.name}")
        print(f"To: {email.to}")
        print(f"Subject: {email.subject}")
        print(f"\nBody:\n{email.body}")
    
    print(f"\n{'=' * 70}")
    print("Demo complete!")
    print("=" * 70)


if __name__ == "__main__":
    demo_collections_agent()