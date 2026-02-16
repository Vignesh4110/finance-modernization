"""
Legacy Code Documenter - AI-Powered RPGLE Documentation Generator
Uses Groq (Llama 3) for free, fast LLM inference
"""

import os
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime

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


class LegacyCodeDocumenter:
    """
    Documents legacy AS400 RPGLE/CL programs using AI.
    Generates documentation, flowcharts, and modernization plans.
    Uses Groq (Llama 3) for free LLM inference.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the documenter."""
        self.api_key = api_key or os.getenv("GROQ_API_KEY")
        
        if GROQ_AVAILABLE and self.api_key:
            self.client = Groq(api_key=self.api_key)
            self.demo_mode = False
        else:
            self.client = None
            self.demo_mode = True
            
        self.docs_dir = PROJECT_ROOT / "docs" / "legacy_system"
        
    def read_program_spec(self, program_name: str) -> Optional[str]:
        """Read a program specification file."""
        # Look for markdown specs
        spec_path = self.docs_dir / "rpgle_specs" / f"{program_name}.md"
        if spec_path.exists():
            return spec_path.read_text()
            
        # Look for CL job docs
        cl_path = self.docs_dir / "cl_jobs" / f"{program_name}.md"
        if cl_path.exists():
            return cl_path.read_text()
            
        return None
    
    def generate_documentation(self, program_spec: str, program_name: str) -> dict:
        """Generate comprehensive documentation for a program."""
        if self.demo_mode:
            return self._demo_documentation(program_spec, program_name)
            
        prompt = f"""You are an expert in AS400/IBM i systems documentation. 
Analyze the following program specification and generate comprehensive documentation.

PROGRAM SPECIFICATION:
{program_spec}

Generate documentation with these sections:
1. EXECUTIVE SUMMARY - Brief overview for business stakeholders
2. TECHNICAL OVERVIEW - Detailed technical description
3. DATA FLOW - Input/output data and transformations
4. BUSINESS RULES - Key business logic implemented
5. DEPENDENCIES - Files, programs, and external systems used
6. ERROR HANDLING - How errors are managed
7. MODERNIZATION RECOMMENDATIONS - How to migrate this to modern stack

Format as Markdown."""

        response = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are an AS400/IBM i expert who creates clear, comprehensive technical documentation."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=2000
        )
        
        documentation = response.choices[0].message.content
        
        return {
            "program_name": program_name,
            "documentation": documentation,
            "generated_at": datetime.now().isoformat(),
            "demo_mode": False
        }
    
    def _demo_documentation(self, program_spec: str, program_name: str) -> dict:
        """Generate demo documentation without API calls."""
        doc = f"""# {program_name} - Program Documentation

## Executive Summary
This program is part of the legacy AS400 financial system. It handles 
critical AR processing functions including invoice management and 
payment application.

## Technical Overview
- **Language**: RPGLE/SQLRPGLE
- **Type**: Batch Processing Program
- **Schedule**: Nightly via WRKJOBSCDE
- **Runtime**: Approximately 45-90 minutes depending on volume

## Data Flow
```
Input:
├── ARMAS (AR Invoice Master)
├── CUSMAS (Customer Master)
└── PAYTRAN (Payment Transactions)

Processing:
├── Validate open invoices
├── Calculate aging buckets
├── Apply business rules
└── Update balances

Output:
├── Updated ARMAS records
├── GL journal entries (GLJRN)
└── AR Aging Report (spool file)
```

## Business Rules
1. **Payment Terms**: Net 15/30/45/60 based on customer segment
2. **Credit Hold**: Triggered when AR > 120% of credit limit
3. **Aging Calculation**: Based on invoice due date vs current date
4. **Write-off Policy**: Invoices > 180 days flagged for review

## Dependencies
- Physical Files: ARMAS, CUSMAS, PAYTRAN, GLJRN
- Programs: AR002R (Payment Application), GL001R (GL Posting)
- Data Areas: ARCTL (AR Control parameters)

## Error Handling
- Job log messages for all critical errors
- QHST logging for audit trail
- Email notification to AR team on failure

## Modernization Recommendations

### Short Term (Lift and Shift)
- Extract to Python scripts maintaining same logic
- Replace file I/O with database queries
- Add structured logging

### Medium Term (Refactor)
- Convert to dbt models for transformations
- Use Airflow for orchestration
- Add data quality tests

### Long Term (Re-architect)
- Event-driven processing with CDC
- Real-time aging calculations
- ML-based collection prioritization

---
*Generated by Legacy Code Documenter*
*Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        return {
            "program_name": program_name,
            "documentation": doc,
            "generated_at": datetime.now().isoformat(),
            "demo_mode": True
        }
    
    def generate_flowchart(self, program_spec: str, program_name: str) -> str:
        """Generate a Mermaid flowchart for the program."""
        if self.demo_mode:
            return self._demo_flowchart(program_name)
            
        prompt = f"""Analyze this AS400 program specification and create a 
Mermaid flowchart showing the program flow.

PROGRAM SPECIFICATION:
{program_spec}

Generate ONLY the Mermaid diagram code, starting with 'flowchart TD'.
Do not include any explanation or markdown code blocks."""

        response = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You create Mermaid flowcharts. Return only the diagram code."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1000
        )
        
        flowchart = response.choices[0].message.content.strip()
        # Clean up any markdown
        flowchart = flowchart.replace("```mermaid", "").replace("```", "").strip()
        return flowchart
    
    def _demo_flowchart(self, program_name: str) -> str:
        """Generate demo flowchart."""
        return f"""flowchart TD
    A[Start {program_name}] --> B[Initialize Parameters]
    B --> C[Open Files]
    C --> D{{Records to Process?}}
    D -->|Yes| E[Read Invoice Record]
    E --> F[Validate Data]
    F --> G{{Valid?}}
    G -->|Yes| H[Calculate Aging]
    G -->|No| I[Log Error]
    I --> D
    H --> J[Apply Business Rules]
    J --> K[Update Record]
    K --> L[Write GL Entry]
    L --> D
    D -->|No| M[Close Files]
    M --> N[Generate Report]
    N --> O[Send Notifications]
    O --> P[End]
"""
    
    def generate_migration_plan(self, program_spec: str, program_name: str) -> dict:
        """Generate a migration plan to modern stack."""
        if self.demo_mode:
            return self._demo_migration_plan(program_name)
            
        prompt = f"""You are a modernization architect. Create a detailed 
migration plan to convert this AS400 program to a modern cloud-native stack.

PROGRAM SPECIFICATION:
{program_spec}

TARGET STACK:
- Python for extraction
- dbt for transformations
- Airflow for orchestration
- PostgreSQL/Snowflake for storage
- ML models for intelligence

Create a phased migration plan with:
1. Phase 1: Assessment & Planning (2 weeks)
2. Phase 2: Parallel Development (4 weeks)
3. Phase 3: Testing & Validation (2 weeks)
4. Phase 4: Cutover & Monitoring (1 week)

Include specific tasks, risks, and success criteria. Format as Markdown."""

        response = self.client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a senior modernization architect creating detailed migration plans."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=2000
        )
        
        return {
            "program_name": program_name,
            "migration_plan": response.choices[0].message.content,
            "generated_at": datetime.now().isoformat()
        }
    
    def _demo_migration_plan(self, program_name: str) -> dict:
        """Generate demo migration plan."""
        plan = f"""# Migration Plan: {program_name}

## Overview
Migration of legacy RPGLE batch processing to cloud-native Python/dbt/Airflow stack.

## Phase 1: Assessment & Planning (2 weeks)

### Week 1: Discovery
- [ ] Document all program dependencies
- [ ] Map data flows and transformations
- [ ] Identify business rules and edge cases
- [ ] Interview subject matter experts

### Week 2: Architecture
- [ ] Design target state architecture
- [ ] Define data models (dbt)
- [ ] Create Airflow DAG structure
- [ ] Establish testing strategy

## Phase 2: Parallel Development (4 weeks)

### Week 3-4: Core Development
- [ ] Build extraction scripts (Python)
- [ ] Create dbt staging models
- [ ] Develop dbt mart models
- [ ] Implement Airflow DAGs

### Week 5-6: Integration
- [ ] Connect to source systems
- [ ] Implement error handling
- [ ] Add logging and monitoring
- [ ] Build reconciliation reports

## Phase 3: Testing & Validation (2 weeks)

### Week 7: Functional Testing
- [ ] Unit tests for each component
- [ ] Integration testing
- [ ] Data reconciliation (legacy vs new)
- [ ] Performance benchmarking

### Week 8: UAT
- [ ] Business user acceptance testing
- [ ] Edge case validation
- [ ] Load testing
- [ ] Failover testing

## Phase 4: Cutover & Monitoring (1 week)

### Cutover Activities
- [ ] Final data sync
- [ ] Switch job scheduler
- [ ] Monitor first production run
- [ ] Validate outputs

### Post-Cutover
- [ ] 24/7 monitoring for 48 hours
- [ ] Daily reconciliation for 1 week
- [ ] Knowledge transfer to ops team
- [ ] Decommission legacy job

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Data discrepancies | Parallel run with reconciliation |
| Performance issues | Load testing, scaling plan |
| Business rule gaps | SME reviews, comprehensive testing |
| Rollback needed | Keep legacy job ready for 30 days |

## Success Criteria
- 100% data accuracy vs legacy
- Processing time <= legacy time
- Zero critical defects in UAT
- All business rules validated
"""
        return {
            "program_name": program_name,
            "migration_plan": plan,
            "generated_at": datetime.now().isoformat()
        }
    
    def document_program(self, program_name: str) -> dict:
        """Full documentation workflow for a program."""
        print(f"\n{'='*60}")
        print(f"DOCUMENTING: {program_name}")
        print(f"{'='*60}")
        
        # Read spec
        spec = self.read_program_spec(program_name)
        if not spec:
            # Use a generic spec for demo
            spec = f"Legacy {program_name} program for AR processing"
            print(f"⚠️  No spec file found, using generic description")
        else:
            print(f"✓ Found program specification")
        
        # Generate documentation
        print("\n📝 Generating documentation...")
        docs = self.generate_documentation(spec, program_name)
        
        # Generate flowchart
        print("📊 Generating flowchart...")
        flowchart = self.generate_flowchart(spec, program_name)
        
        # Generate migration plan
        print("🚀 Generating migration plan...")
        migration = self.generate_migration_plan(spec, program_name)
        
        result = {
            "program_name": program_name,
            "documentation": docs["documentation"],
            "flowchart": flowchart,
            "migration_plan": migration["migration_plan"],
            "generated_at": datetime.now().isoformat(),
            "demo_mode": self.demo_mode
        }
        
        print(f"\n✅ Documentation complete for {program_name}")
        return result


def demo_legacy_documenter():
    """Demonstrate the Legacy Code Documenter."""
    print("\n" + "="*70)
    print("LEGACY CODE DOCUMENTER - AI-Powered RPGLE Documentation")
    print("="*70)
    
    documenter = LegacyCodeDocumenter()
    
    if documenter.demo_mode:
        print("\n⚠️  Running in DEMO MODE (no API key)")
        print("   Set GROQ_API_KEY for AI-powered documentation")
    else:
        print("\n✅ Running with Groq (Llama 3.1 70B)")
    
    # Document a sample program
    result = documenter.document_program("AR001R_invoice_processing")
    
    # Print results
    print("\n" + "="*60)
    print("GENERATED DOCUMENTATION")
    print("="*60)
    print(result["documentation"][:2000] + "..." if len(result["documentation"]) > 2000 else result["documentation"])
    
    print("\n" + "="*60)
    print("FLOWCHART (Mermaid)")
    print("="*60)
    print(result["flowchart"])
    
    print("\n✅ Legacy Code Documenter demo complete!")
    

if __name__ == "__main__":
    demo_legacy_documenter()