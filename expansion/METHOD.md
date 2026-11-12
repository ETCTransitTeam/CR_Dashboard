ETC INSTITUTE EXPANSION — METHOD AND USE

1. Start the local server with python app.py. A browser window opens for this computer only. Select the input workbook and click Run expansion. It needs the prepared O2O, OD, APC and SAMPLING sheets used by the existing model. You can replace individual tables on the Inputs tab with CSV or Excel files having the same headers.

2. Review Checks. In Expansion cells, inspect the original fitted ridership, adjusted ridership, survey counts, raw weight, final weight and normalized final weight. Check multiple rows and choose Pool selected cells to revise a group. Set weight applies a reviewed numeric weight. Reconciliations update after every edit.

3. Export Excel + CSV downloads a ZIP containing the run files. The Excel workbook includes EXPORT in the familiar column order, survey-level weights, route-time totals, adjustments, checks and a run summary. Exports are calculated snapshots. Rerun the application when inputs change; editing an exported snapshot does not rerun the model.

EXPANSION

The engine reproduces the Power Query sequence: count O2O cells, add missing triangular segment cells for time periods 1–4, seed ridership from O2O shares and APC boarding totals, then perform exactly ten alternating alighting and boarding adjustments. Each route gets its own segment list from APC. Missing combinations are generated dynamically for every boarding/alighting pair with alighting >= boarding; no Fill Up/Fill Down column list needs maintenance. Different routes may have different segment counts. The original missing-cell join adds one record per matching OD survey, or one dummy record if there is no survey. This behavior is preserved.

Automatic pooling combines the nearest segment groups by boarding/alighting segment distance, with a stable tie order, until a group has survey support and its pooled weight is greater than 0.25 and meets its route sampling limit. Pools never cross route, direction or time period. This implements the nearby-segment policy; it may choose different groups from a person's judgment. Review the adjustment table. Cases with no available surveys or infeasible limits remain flagged.

The visible Sampling rate is entered in percentage points: 5 means 5%. It controls both automatic pooling and the high-weight check. Guideline weight factor = (1 / (sampling percentage / 100)) * 3 = 300 / sampling percentage. Examples: 5% gives 60; 4.25% gives 70.588235; 10% gives 30. The guideline is displayed beside the input. Weights above the guideline trigger pooling, rather than being capped. Reviewed Excel weights and manual overrides are retained and flagged if they exceed the guideline. Original route rates are retained for reference, but the application uses the visible project rate for the effective limit.

EXISTING EXCEL ADJUSTMENTS

An optional project-specific adjustment file preserves the reviewed EXPORT pools, fixed weights and ridership transfers. It applies only when all four source tables match its stored input fingerprint. Exact pasted EXPORT precision is preserved for this matching run; the newly calculated Power BI replacement remains visible in the original fitted-ridership column. Changed inputs use freshly calculated values and automatic pooling instead. Old constants and transfers are never silently carried into new inputs.

CHECKS

The existing Excel checks are recomputed locally: M, Q, U, X, Z, AC, AG and AJ. Excel rounding, truncation and its 15-digit decimal comparison behavior are reproduced. Additional checks expose missing sampling limits, unavailable survey weights, final weights at or below 0.25, and final weights exceeding route limits. A weight of exactly 0.25 triggers pooling; zero-weight cells with surveys also qualify. Empty cells with neither ridership nor surveys do not require pooling. A run can export with unresolved checks so you can review the evidence.

SCOPE

This version supports the supplied prepared-table format, one day type per run, nonnegative APC counts (blank values reported as unavailable), and any number of contiguous segments numbered from 1. It stops on unsupported segment structures rather than inventing a changed expansion method. Raw survey cleaning, stop-to-segment assignment and GTFS preparation are upstream of this model.

Everything runs locally. Power BI and Excel are not needed to calculate. This programmer handoff uses Python and public packages listed in requirements.txt. No Power BI, Excel, Node or Codex installation is required. See README.md for local setup and DEPLOYMENT.md before hosting.


AREAS TO FIX

Areas to fix compares total APC boarding ridership across all segments with total OD surveys for each route, direction and time period. It exports one row per overall group with positive ridership and no surveys, or positive surveys and no ridership. Missing APC boarding inputs are listed separately as unavailable. A segment-to-segment gap does not trigger this list if both overall group totals are positive. Export areas to fix downloads an Excel workbook with Areas to fix, APC, and APC merge log; the full Excel/CSV bundle also includes these tables.

Blank APC numbers are not assumed to be zero. The app marks the affected route/time as unavailable and runs the remaining complete groups. The status and run summary identify partial totals; repair missing inputs and rerun for a complete result. Negative and infinite APC values still stop the run.

Areas to fix is evaluated only at the overall route, direction and time-period level. It compares independent APC boarding totals across all segments with total OD surveys. Segment-to-segment gaps are not included. A group with positive totals on both sides is not flagged, even if individual segment cells lack surveys. Missing APC boarding inputs remain separately unavailable.


UPDATED APC DOWNLOAD

Export areas to fix downloads an Excel workbook: first tab Areas to fix, second tab APC (updated values in the original column order), third tab APC merge log. Positive ON and OFF ridership in a time period with zero OD surveys is moved, segment by segment, into the nearest time period with surveys within the same route and direction. Time period 2 prefers 1; time period 3 prefers 4. If the preferred receiving period has no surveys or incomplete APC, use the nearest surveyed time period with complete APC within the same route and direction, measured from the source period. Other equal-distance ties use the earlier period. If no usable surveyed period exists, keep the source unchanged and flag it. Source values become zero and target values receive the transferred amount. The original source workbook is not edited. Groups with missing APC numbers or no surveyed receiving period are left unchanged and logged. Totals for every segment's ON and OFF counts are preserved. These are proposed input corrections; load the APC tab as an input replacement and rerun to calculate revised expansion results.
