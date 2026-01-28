UK Companies House PDF notes

Sources reviewed (official forms):
- CS01 confirmation statement (Companies House form)
- AR01 annual return (Companies House form)

Observed header structure and key labels:
- CS01 includes a "Company details" section that lists:
  - "Company name in full"
  - "Company number"
  - "Confirmation date" with a dd mm yyyy layout
- AR01 includes a "Company details" section that lists:
  - "Company name in full"
  - "Company number"
  - "Date of this return" with a dd mm yyyy layout

Parsing implications:
- Company name and number appear as labeled fields near the top of the form.
- Filing date is labeled as "Confirmation date" (CS01) or "Date of this return"
  (AR01), so parsing should look for these label variants.
