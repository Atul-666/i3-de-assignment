# i3 Digital Health — Data Engineering SIP Assignment

**Candidate:** Atul
**Problem attempted:** Problem 1 + Problem 2 (+ Problem 3)

---

## Live Dashboards

🔗 **[Problem 1 — Patient Risk Dashboard](https://atul-666.github.io/i3-de-assignment/harmon_risk_dashboard.html)**
🔗 **[Problem 2 — Drug Safety Pipeline](https://atul-666.github.io/i3-de-assignment/index.html)**

> Both open directly in browser — no installation needed.

---

## What I Built

A **post-discharge readmission risk pipeline** for a real-seeming heart failure patient — Robert "Bob" Harmon — who was readmitted within 30 days of hospital discharge.

The system ingests three data sources, cross-references them, and generates a day-by-day alert log showing exactly when a care team should have intervened — and why nobody did.

---

## The Three Input Files

| File | What it contains |
| --- | --- |
| `discharge_summary_harmon.pdf` | Hospital discharge summary — diagnoses, medications, alert thresholds set by Dr. Nair |
| `wearable_export_bob_harmon.csv` | 28 days of smartwatch data — weight, heart rate, SpO₂, steps, sleep, irregular HR events |
| `pharmacy_feed_harmon.json` | Pharmacy dispensing records — what was filled, when, and what was in the notes |

---

## The Three Output Files

| File | What it is |
| --- | --- |
| `risk_pipeline.py` | Python script — ingests all 3 sources, computes daily risk scores, prints alert report, saves CSV |
| `risk_alerts.csv` | Structured alert log — every flag the system would have fired, day by day |
| `harmon_risk_dashboard.html` | Interactive clinical dashboard — reads CSV and JSON live in the browser, computes and visualises everything dynamically |

---

## Key Findings

The readmission was not a mystery. It was a cascade of silent failures across three disconnected systems:

1. **Spironolactone never dispensed (Day 2)** — A new medication added specifically to prevent fluid reaccumulation was never filled. No one was notified.
2. **Carvedilol substituted without prescriber confirmation (Day 1)** — Pharmacy swapped his CHF medication for a non-equivalent drug. A voicemail was left; no callback was documented.
3. **Weight climbing silently** — Wearable recorded +3.9 kg over 28 days, crossing the discharge alert threshold multiple times. No automated alert existed.
4. **Apixaban gap (Day 22)** — Patient ran out of his blood thinner and went several days without it. Noted in pharmacy records; never escalated.
5. **Functional collapse** — Steps dropped from ~1,600/day to under 200/day. SpO₂ fell from 96% to 90%. Resting HR climbed from 75 to 96 bpm.

**Earliest actionable alert: Day 2 (March 14) — pharmacy data alone.**
**Composite risk score crossed intervention threshold: Day 15 (March 27) — ~2 weeks before readmission.**

---

## How to Run

### Python Pipeline

```
pip install pandas numpy matplotlib
python risk_pipeline.py
```

Prints the full alert report to terminal and saves `risk_alerts.csv`.

### Live Dashboard

```
python -m http.server 8000
```

Then open: `http://localhost:8000/harmon_risk_dashboard.html`

The dashboard reads `wearable_export_bob_harmon.csv` and `pharmacy_feed_harmon.json` directly — no hardcoded values. Everything (KPIs, charts, risk scores, timeline, findings, alert log) is computed live from the source files.

---

## Design Decisions & Assumptions

* **Rule-based scoring, not ML** — Only 28 rows of data for one patient. A machine learning model would be fake science here. The alert thresholds are extracted directly from Dr. Nair's discharge instructions; the point weights within the composite score are heuristic assumptions based on clinical reasoning (weight gain weighted highest as the primary CHF signal).
* **Cross-source approach** — The insight only emerges when pharmacy, wearable, and discharge plan are read together. No single source tells the full story.
* **Discharge date hardcoded** — March 12, 2024 is taken from the PDF. It does not appear in the CSV or JSON so cannot be read dynamically.
* **Readmission date estimated** — The assignment states readmission happened within 30 days. The exact date is not in the data; the dashboard uses the last wearable sync date as a proxy.

---

## Planned Extensions

* **AI Clinical Assistant** — a conversational chatbot grounded in the patient's actual data, allowing care teams to ask questions like *"what's the most urgent issue?"* or *"when should an alert have fired?"* Prototyped using the Claude API. Requires a backend proxy for secure deployment on a public URL — a production consideration intentionally left out of this submission to avoid API key exposure on a public GitHub repo.

---

## Problem 2 — What Side Effects Does Furosemide Cause?

### Why Furosemide?

Furosemide is the water pill Bob Harmon was prescribed when he left hospital in Problem 1. It's the primary drug used to manage fluid buildup in heart failure patients. I chose it deliberately — it connects both problems. In a real health system, a care team monitoring Bob would want to know exactly what adverse events are reported for this drug in the real world.

### What I built

A pipeline that pulls real adverse event reports from the FDA's public database (OpenFDA) for Furosemide and turns the messy raw data into something clean and useful.

The Python script (`openfda_pipeline.py`) does this:
- Pulls reports from OpenFDA 100 at a time (the API's max per request without a key)
- Handles the API being slow or grumpy — retries up to 3 times, waits if it hits a rate limit
- Flattens deeply nested JSON into a simple flat table a human can actually read
- Removes duplicate reports
- Saves both the raw data (JSON) and the cleaned version (CSV)

Run it with:
```
pip install requests
python openfda_pipeline.py
```

You'll get two files:
- `openfda_Furosemide_raw.json` — exactly what the API returned, untouched
- `openfda_Furosemide_cleaned.csv` — flat, clean, ready to analyse

### The live dashboard

The `index.html` does the same thing but right in your browser — no Python, no terminal. Just hit "Run Pipeline," watch the records load in real time, and download the results when done.

It also generates 4 charts automatically:
- Timeline of when reports were filed
- Top 10 most reported reactions
- Age distribution of affected patients
- Seriousness breakdown by sex

### Things I want to be upfront about

* **The 25,000 record ceiling** — OpenFDA won't let you fetch more than 25,000 records through the search API. Confirmed directly against their official docs. For anything larger, you'd need their bulk data files instead.
* **This data doesn't prove the drug caused anything** — an adverse event report just means someone reported a reaction while on the drug. Causality is not established. Noted clearly in the Explanation tab.
* **Missing demographics are kept, not dropped** — some reports don't include age or sex. Mapped to "Unknown" and retained rather than silently removed, which would skew the numbers.
* **Built with Gemini** — the pipeline structure, pagination logic, and browser-side JavaScript were co-developed with Gemini. The 25K limit was verified independently against OpenFDA's documentation, sex code mapping confirmed against FAERS standards, and retry logic tested manually. Drug choice, data handling decisions, and what to flag as limitations were my own.

---


## Repository Structure

```
i3-de-assignment/
├── Problem-1/
│   ├── harmon_risk_dashboard.html
│   ├── risk_pipeline.py
│   ├── risk_alerts.csv
│   ├── wearable_export_bob_harmon.csv
│   ├── pharmacy_feed_harmon.json
│   └── discharge_summary_harmon.pdf
├── Problem-2/
│   ├── openfda_pipeline.py
│   ├── openfda_Furosemide_cleaned.csv
│   └── index.html
└── README.md
```

---

## Problem 3

A short Loom video (≤2 mins) explaining what was built — addressed to a non-technical audience, no jargon — is included in the submission email.
