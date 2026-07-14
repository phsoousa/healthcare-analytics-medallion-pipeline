---
name: claims-cost-review
description: Produce a standardized claims-versus-treatment-cost review for a hospital
  department or time period. Use this skill when the user asks for a "claims review",
  "cost vs approved analysis", "department financial summary", or wants claimed amount,
  approved amount, and treatment cost compared for a department.
---

# Claims vs treatment cost review

When the user invokes this skill, follow these steps in order.

1. Use the "Query structured data" tool (the HEALTHCARE_SEMANTIC_VW semantic view)
   to retrieve, for the requested department and time period:
   - NUM_CLAIMS
   - TOTAL_CLAIM_AMOUNT
   - TOTAL_APPROVED_AMOUNT
   - TOTAL_TREATMENT_COST
   - NUM_ADMISSIONS
   If the user did not name a department or period, ask them once before continuing.

2. Compute the approval rate as TOTAL_APPROVED_AMOUNT divided by TOTAL_CLAIM_AMOUNT,
   expressed as a percentage.

3. Compare TOTAL_TREATMENT_COST against TOTAL_APPROVED_AMOUNT. If treatment cost is
   greater than the approved amount, clearly flag the department as running at a
   financial shortfall.

4. Present the results as a small markdown table with one row per department (or the
   single requested department), followed by 2-3 short bullet insights.

5. Never invent or estimate numbers. Every figure in the answer must come from the
   tool output. If a value is missing, say so explicitly rather than guessing.