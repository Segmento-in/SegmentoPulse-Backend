---
trigger: always_on
---

RULE: Segmento Pulse Newsletter Core Logic & Stability

CONTEXT: Segmento Pulse features a highly specific, time-windowed newsletter delivery system. This system is critical for production stability and rate-limit management.

CORE LOGIC (Do Not Modify):

Data Retrieval Strategy: The system MUST use get_articles_with_queries. NEVER attempt to fetch the entire collection (get_all_articles). All fetches must be strictly bound by createdAt timestamps.

Daily Schedule & Time Windows (IST):

Morning (07:00 IST): Query Range = 23:00 (Previous Night) to 07:00 (Current).

Afternoon (14:00 IST): Query Range = 07:00 to 14:00.

Evening (19:00 IST): Query Range = 14:00 to 19:00.

Aggregation Schedule:

Weekly: Query Range = Last 7 Days. Limit = Top 10.

Monthly: Query Range = Last 30 Days. Limit = Top 25.

Content Diversity Algorithm: Newsletters must follow a Round-Robin Category Selection.

Target exactly 5 articles per email to maintain a <100KB size.

Articles must be pulled from a mix of categories (e.g., AI, Cloud, Data) rather than a single source.

Visual Identity (Medium-Style):

All newsletters must use the Editorial Digest template.

Constraint: Summaries MUST be truncated to 160 characters max + "...".

Constraint: Each item must include a Category Tag, Headline, and a "Read on Segmento Pulse" CTA link.

PROTECTION POLICY: If any new feature or bug fix requires changes to the newsletter_service or database query logic, Antigravity MUST verify that these time-window boundaries and the Round-Robin selection logic remain intact. DO NOT allow the agent to revert to generic "fetch all" functions.