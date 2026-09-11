**Source visual truth**

- `/Users/theresapappas/Downloads/Screenshot 2026-09-10 at 8.46.19 PM.png`
- HomeWorks Estimate v4 email shown on an iPhone in Gmail.
- `/Users/theresapappas/Downloads/Sign Service Agreement  Pappas & Co. Landscaping 3.png`
- Customer-facing agreement screenshot showing the Estimate Reference card before this refinement.

**Implementation evidence**

- `/private/tmp/yarddesk-contract-mobile-final.png`
- `/private/tmp/yarddesk-contract-desktop.png`
- `/private/tmp/yarddesk-contract-design-comparison.png`
- `/private/tmp/yarddesk-signature-font-gistesy-style-mobile.png`
- `/private/tmp/agreement-email-summary-fixed.png`
- `/private/tmp/yarddesk-estimate-reference-two-column-focused.png`
- `/private/tmp/yarddesk-estimate-reference-comparison.png`

**Viewport and normalization**

- Mobile CSS viewport: 390 x 844. Browser content capture: 375 px wide at device scale 1.
- Estimate Reference focused capture: 375 x 750 px. Source screenshot: 1320 x 2868 px. Combined comparison: 1265 x 1249 px.
- Source image: 1320 x 2868 px, including the iPhone Gmail interface and browser chrome.
- Final implementation full-page capture: 375 x 8965 px. The page is longer because it contains the complete legal agreement and signature form.
- Desktop CSS viewport: 1440 x 1000. Browser content capture: 1425 x 689 px.
- The combined comparison scales each artifact to the same column width and compares the shared top-of-content region. Device and Gmail chrome are excluded from fidelity judgments.
- State: unsigned Estimate #1676 with Theresa Pappas, 1513 Lincoln Avenue, Mowing (Weekly), and a $1.08 total.

**Full-view comparison evidence**

- The implementation uses the same dark charcoal header, lime-and-white logo, pale green gradient, dark green headings, compact gray body copy, lime accents, rounded white surfaces, and generous white space as Estimate v4.
- The agreement page keeps its additional workflow information and legal content because those are required for signing, but it no longer looks like a separate product.
- Mobile and desktop captures show no horizontal overflow or clipped primary controls.

**Focused region comparison evidence**

- Header and hero: the logo treatment, charcoal header, pale green gradient, uppercase eyebrow, and headline hierarchy match the email system.
- Estimate summary: the address and service use lime rules, while the total sits in the same pale green treatment used by the email.
- Agreement and signature cards: typography, borders, radii, colors, and button styling use the same tokens as the email.

**Required fidelity surfaces**

- Fonts and typography: Helvetica and Arial match the email-safe Estimate v4 stack. Headings, labels, body copy, weights, line height, and wrapping are consistent and readable at mobile and desktop sizes.
- Spacing and layout rhythm: the 760 px content width, 16 to 30 px padding, 18 to 24 px gaps, rounded cards, and section spacing follow the email proportions without crowding the legal content.
- Colors and visual tokens: `#1f2933`, `#2e403d`, `#c9dd80`, `#f7f9f5`, and `#edf3e6` match the Estimate v4 system and maintain readable contrast.
- Image quality and asset fidelity: the actual YardDesk email logo asset is used. No replacement illustration, placeholder logo, or generated asset is present.
- Copy and content: customer-facing labels now consistently say Estimate, and the agreement introduction and references use estimate language throughout.
- Typed signature: Ms Madi provides the thin, flowing, connected style requested as a licensed Google Fonts alternative to Gistesy, while retaining a readable fallback stack.

**Comparison history**

- First pass P2: the fixed signing bar covered content while scrolling on mobile, and the agreement used a nested scroll area. Evidence: `/private/tmp/yarddesk-contract-mobile-v2.png`.
- Fix: moved the signing action into the normal document flow, removed the internal agreement scrollbar, reduced bottom padding, and made the mobile action card stack cleanly.
- Post-fix evidence: `/private/tmp/yarddesk-contract-mobile-final.png` shows the full agreement, signature form, and signing action without overlap.
- First pass P3: signature mode labels used emoji that did not belong to the Estimate v4 system.
- Fix: changed them to clean text labels, Draw Signature and Type Signature.
- Agreement email P2: its summary columns stacked on mobile, so the lime divider no longer separated the summary copy from the estimate details like it does in HomeWorks Estimate v4.
- Fix: kept the two summary cells side by side at the mobile breakpoint. The agreement summary remains left of the lime divider, with Estimate and Total on the right. Evidence: `/private/tmp/agreement-email-summary-fixed.png`.
- Agreement page P2: Service Address and Services stacked as separate rows on mobile, while the estimate email uses a compact side-by-side summary divided by one lime rule. The estimate reference number was also visually oversized.
- Fix: retained a 48/52 two-column grid on mobile, placed the lime divider between the columns, reduced the detail copy to 12 px, kept the formatted street and city/state/ZIP on exactly two lines, and reduced the reference number to 18 px. Post-fix evidence: `/private/tmp/yarddesk-estimate-reference-two-column-focused.png` and `/private/tmp/yarddesk-estimate-reference-comparison.png`.

**Findings**

- No actionable P0, P1, or P2 visual differences remain.

**Primary interactions tested**

- Loaded the agreement with realistic Estimate #1676 data.
- Switched from Draw Signature to Type Signature.
- Entered a typed signature and checked the consent box.
- Verified the Sign Agreement button becomes enabled.
- Verified no visible error state or loading lock remained.
- Verified desktop layout has no horizontal overflow.

**Follow-up polish**

- P3: a future pass could visually match the post-signing portal-success screen, which was outside this unsigned-agreement comparison.

final result: passed
