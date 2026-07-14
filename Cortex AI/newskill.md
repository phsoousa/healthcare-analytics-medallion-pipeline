---
name: readmission-risk
description: Analyze patient readmission risk by identifying patients readmitted
  within 30 days and surfacing contributing factors. Use this skill when the user
  asks about "readmission risk", "readmitted patients", "30-day readmissions",
  "repeat admissions", or "readmission factors".
---

# Readmission risk analysis

When the user invokes this skill, follow these steps in order.

1. Use the "Query structured data" tool (the HEALTHCARE_SEMANTIC_VW semantic view)
   to identify patients with more than one admission within a 30-day window.
   Retrieve: PATIENT_ID, DEPARTMENT, ADMISSION_DATE, DISCHARGE_DATE,
   LENGTH_OF_STAY, DIAGNOSIS, and INSURANCE_PROVIDER.

2. Group results by department and diagnosis to identify which combinations
   produce the highest readmission counts.

3. Flag any patient with 3 or more admissions in a 90-day period as
   "high frequency" and highlight them separately.

4. Present results in two tables:
   - Department-level summary: department, readmission count, avg length of stay,
     top diagnosis
   - High-frequency patients (if any): patient ID, admission count, departments,
     date range

5. After the tables, provide 2-3 bullet-point insights about patterns
   (e.g., which departments or diagnoses correlate with higher readmission).

6. Never invent or estimate numbers. Every figure must come from the tool output.
   If data is insufficient to determine readmission (e.g., only one admission
   record exists for all patients), state this clearly.
   