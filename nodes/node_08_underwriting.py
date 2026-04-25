# nodes/node_08_underwriting.py
"""
Node 8 — Underwriting Node (The Quant)

Responsibilities per Spec 1.8.2 Section 10, Node 8:
  - Read leads at pipeline_stage='enriched' with node_07_vision complete.
  - Resolve purchase price (asking or proxy).
  - Compute risk_adjusted_contingency per capex_confidence.
  - Compute full cost stack per Spec Section 7.4.
  - Compute max_offer_price via algebraic non-circular formula (Spec 7.5).
  - Compute rule_of_70 sanity check and divergence.
  - Compute execution_score and data_confidence_score.
  - Compute final_rank_score with static penalties (Spec 8.4).
  - Route to approved / manual_review / rejected per routing table (Spec 10 Node 8).
  - Write AcquisitionDecision with recommendation, reason_codes, summary, next_action.
  - Advance pipeline_stage to 'underwritten'.
"""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

from core.database import get_leads_by_stage, upsert_lead
from core.models import AcquisitionDecision, AuditEntry, JCMPropertyState, PipelineError
from core.settings import get_settings

logger = logging.getLogger(__name__)
NODE_NAME = "node_08_underwriting"
NODE_07_NAME = "node_07_vision"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def compute_execution_score(state: JCMPropertyState) -> tuple[int, str]:
    """
    Execution score 0-100 per Spec 1.8.2 Section 8.2.
    Sprint 1: conservative heuristic proxy pending full implementation.
    Factors: property type liquidity, age risk, data completeness.
    """
    settings = get_settings()
    score = settings.UNDERWRITING_BASE_EXECUTION_SCORE
    factors: list[str] = []

    if state.property_type == "SFR":
        score += 10
        factors.append("SFR liquidity +10")
    elif state.property_type in ("Townhome", "Condo"):
        score += 3
        factors.append(f"{state.property_type} liquidity +3")

    if state.year_built:
        if state.year_built < 1940:
            score -= 15
            factors.append("pre-1940 age risk -15")
        elif state.year_built < 1965:
            score -= 8
            factors.append("pre-1965 age risk -8")
    else:
        score -= 5
        factors.append("unknown year_built -5")

    if state.building_sqft is None:
        score -= 10
        factors.append("missing sqft -10")

    if state.beds is None or state.baths is None:
        score -= 5
        factors.append("missing beds/baths -5")

    score = max(0, min(100, score))
    base = settings.UNDERWRITING_BASE_EXECUTION_SCORE
    rationale = f"Base {base} | {' | '.join(factors)} -> {score}"
    return score, rationale


def compute_data_confidence_score(state: JCMPropertyState) -> tuple[int, str]:
    """
    Data confidence score 0-100 per Spec 1.8.2 Section 8.3.
    Spec constraints:
      arv_confidence='low' or no images -> score <= 50
      facts/arv/capex all 'high'        -> score >= 80
    """
    score = 70
    factors: list[str] = []

    if state.arv_confidence == "high":
        score += 15
        factors.append("arv=high +15")
    elif state.arv_confidence == "medium":
        score += 5
        factors.append("arv=medium +5")
    elif state.arv_confidence == "low":
        score -= 30
        factors.append("arv=low -30")

    if state.capex_confidence == "high":
        score += 10
        factors.append("capex=high +10")
    elif state.capex_confidence == "low":
        score -= 20
        factors.append("capex=low -20")

    if not state.image_urls:
        score -= 20
        factors.append("no images -20")
    elif len(state.image_urls) >= 4:
        score += 5
        factors.append("4+ images +5")

    if state.facts_confidence == "high":
        score += 5
        factors.append("facts=high +5")
    elif state.facts_confidence == "low":
        score -= 10
        factors.append("facts=low -10")

    # Spec hard constraints
    if state.arv_confidence == "low" or not state.image_urls:
        score = min(score, 50)
        factors.append("capped at 50")

    if (state.facts_confidence == "high"
            and state.arv_confidence == "high"
            and state.capex_confidence == "high"):
        score = max(score, 80)
        factors.append("floored at 80")

    score = max(0, min(100, score))
    rationale = f"Base 70 | {' | '.join(factors)} -> {score}"
    return score, rationale


# ---------------------------------------------------------------------------
# Failure helper
# ---------------------------------------------------------------------------

def _fail_lead(
    state: JCMPropertyState,
    audit_entries: list[AuditEntry],
    error_code: str,
    message: str,
    review_reason: str,
    rationale: str,
    now_iso: str,
    db_path: Optional[str | Path] = None,
) -> None:
    prior_stage = state.pipeline_stage

    state.errors.append(PipelineError(
        timestamp=now_iso,
        node=NODE_NAME,
        error_code=error_code,
        message=message,
        is_fatal=False,
    ))

    if review_reason not in state.manual_review_reasons:
        state.manual_review_reasons.append(review_reason)

    state.pipeline_stage = "error"
    state.audit_log.extend(audit_entries)
    state.audit_log.append(AuditEntry(
        timestamp=now_iso,
        node=NODE_NAME,
        field_mutated="pipeline_stage",
        value_before=prior_stage,
        value_after="error",
        rationale=rationale,
    ))
    upsert_lead(state, db_path)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_underwriting(
    run_id: str,
    db_path: Optional[str | Path] = None,
) -> dict[str, int]:
    """
    Node 8 — Underwriting Node (The Quant).

    Reads:  pipeline_stage = 'enriched', node_07_vision in data_sources
    Writes:
        All cost fields, ROI, max_offer_price, scores, decision
        pipeline_stage = 'underwritten' — on all routable outcomes
        pipeline_stage = 'error'        — only on missing ARV or exception

    Routing precedence (Spec Section 10 Node 8):
      1. rejected  — ROI < 10% or max_offer <= 0
      2. manual_review — low confidence, proxy pricing, near-threshold ROI, 70-rule divergence
      3. approved  — ROI >= 15%, medium/high confidence bands, no fatal conditions

    Idempotent: skips leads with NODE_NAME already in data_sources.
    """
    settings = get_settings()
    leads = get_leads_by_stage(run_id, "enriched", db_path)

    metrics: dict[str, int] = {
        "processed": 0,
        "approved": 0,
        "manual_review": 0,
        "rejected": 0,
        "skipped": 0,
        "errors": 0,
    }

    for state in leads:
        # --- Idempotency guard ---
        if NODE_NAME in state.data_sources:
            metrics["skipped"] += 1
            continue

        # --- Node ordering guard ---
        if NODE_07_NAME not in state.data_sources:
            metrics["skipped"] += 1
            continue

        metrics["processed"] += 1
        now_iso = utc_now_iso()
        audit_entries: list[AuditEntry] = []

        try:
            # --- Hard gate: risk_adjusted_arv is required ---
            if state.risk_adjusted_arv is None:
                _fail_lead(
                    state, audit_entries,
                    error_code="UNDERWRITING_INPUT_MISSING",
                    message=f"risk_adjusted_arv is None for property_id={state.property_id}",
                    review_reason="UNDERWRITING_INPUT_MISSING",
                    rationale="Cannot underwrite without risk_adjusted_arv",
                    now_iso=now_iso,
                    db_path=db_path,
                )
                metrics["errors"] += 1
                continue

            arv = state.risk_adjusted_arv

            # --- Soft gate: total_rehab_estimate ---
            total_rehab = state.total_rehab_estimate
            if total_rehab is None:
                _fail_lead(
                    state, audit_entries,
                    error_code="MISSING_REHAB_ESTIMATE",
                    message=(
                        "total_rehab_estimate is None (vision analysis may have failed). "
                        "Cannot underwrite without rehab cost estimate."
                    ),
                    review_reason="MISSING_REHAB_ESTIMATE",
                    rationale="No rehab estimate available — cannot calculate MAO",
                )
                upsert_lead(state, db_path)
                metrics["errors"] += 1
                continue

            # Step 1: Purchase price
            if not state.estimated_purchase_price:
                purchase_price = round(arv * settings.PURCHASE_FACTOR_PROXY, 2)
                state.estimated_purchase_price = purchase_price
                state.purchase_price_method = "proxy_arv_factor"
                audit_entries.append(AuditEntry(
                    timestamp=now_iso,
                    node=NODE_NAME,
                    field_mutated="estimated_purchase_price",
                    value_before=None,
                    value_after=purchase_price,
                    rationale=(
                        f"Off-market proxy: risk_adjusted_arv {arv:,.2f} "
                        f"x PURCHASE_FACTOR_PROXY {settings.PURCHASE_FACTOR_PROXY} "
                        f"= {purchase_price:,.2f}"
                    ),
                ))
            else:
                purchase_price = state.estimated_purchase_price

            # Step 2: Contingency (risk-adjusted per capex_confidence)
            base_contingency = round(total_rehab * settings.CONTINGENCY_PCT, 2)
            capex_conf = state.capex_confidence or "low"
            conf_multiplier = settings.CONTINGENCY_MULTIPLIER_LOW_CONF if capex_conf == "low" else 1.0
            risk_adjusted_contingency = round(base_contingency * conf_multiplier, 2)

            # base_contingency is a local var (not on JCMPropertyState)
            state.risk_adjusted_contingency = risk_adjusted_contingency

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="risk_adjusted_contingency",
                value_before=None,
                value_after=risk_adjusted_contingency,
                rationale=(
                    f"base_contingency = {total_rehab:,.2f} x {settings.CONTINGENCY_PCT} "
                    f"= {base_contingency:,.2f}; "
                    f"{conf_multiplier}x multiplier (capex_confidence={capex_conf}) "
                    f"-> {risk_adjusted_contingency:,.2f}"
                ),
            ))

            # Step 3: Cost components per Spec Section 7.4
            loan_amount = round(purchase_price * (1 - settings.DOWN_PAYMENT_PCT), 2)
            origination_points_cost = round(
                loan_amount * settings.ORIGINATION_POINTS_PCT, 2
            )
            monthly_interest = round(
                loan_amount * settings.HARD_MONEY_RATE_MONTHLY, 2
            )
            holding_costs = round(
                origination_points_cost + (monthly_interest * settings.HOLDING_MONTHS), 2
            )
            closing_costs = round(purchase_price * settings.CLOSING_COST_PCT, 2)
            selling_costs = round(arv * settings.SELLING_COST_PCT, 2)

            # closing/holding/selling cost estimates are local vars used in
            # total_project_cost calc — not persisted fields on JCMPropertyState

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="closing_cost_estimate",
                value_before=None,
                value_after=closing_costs,
                rationale=(
                    f"closing = purchase {purchase_price:,.2f} "
                    f"x {settings.CLOSING_COST_PCT} = {closing_costs:,.2f}"
                ),
            ))
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="holding_cost_estimate",
                value_before=None,
                value_after=holding_costs,
                rationale=(
                    f"origination {origination_points_cost:,.2f} + "
                    f"interest ({monthly_interest:,.2f}/mo x {settings.HOLDING_MONTHS} mo) "
                    f"= {holding_costs:,.2f}"
                ),
            ))
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="selling_cost_estimate",
                value_before=None,
                value_after=selling_costs,
                rationale=(
                    f"selling = arv {arv:,.2f} "
                    f"x {settings.SELLING_COST_PCT} = {selling_costs:,.2f}"
                ),
            ))

            # Step 4: Total project cost per Spec Section 7.4
            total_project_cost = round(
                purchase_price
                + total_rehab
                + risk_adjusted_contingency
                + closing_costs
                + holding_costs
                + selling_costs,
                2,
            )
            state.total_project_cost = total_project_cost

            # Step 5: Total invested cash (cash-on-cash) per Spec Section 7.4
            down_payment = round(purchase_price * settings.DOWN_PAYMENT_PCT, 2)
            rehab_cash_exposure = round(
                total_rehab * settings.REHAB_CASH_EXPOSURE_PCT, 2
            )
            total_invested_cash = round(
                down_payment
                + rehab_cash_exposure
                + closing_costs
                + holding_costs
                + risk_adjusted_contingency,
                2,
            )
            state.total_invested_cash = total_invested_cash

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="total_invested_cash",
                value_before=None,
                value_after=total_invested_cash,
                rationale=(
                    f"down {down_payment:,.2f} + rehab_exposure {rehab_cash_exposure:,.2f} "
                    f"+ closing {closing_costs:,.2f} + holding {holding_costs:,.2f} "
                    f"+ contingency {risk_adjusted_contingency:,.2f} "
                    f"= {total_invested_cash:,.2f}"
                ),
            ))

            # Step 6: Profit and ROI
            expected_profit = round(arv - total_project_cost, 2)
            state.expected_profit = expected_profit

            if total_invested_cash > 0:
                expected_roi = round(expected_profit / total_invested_cash, 4)
            else:
                expected_roi = 0.0
            state.expected_roi = expected_roi

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="expected_roi",
                value_before=None,
                value_after=expected_roi,
                rationale=(
                    f"profit = arv {arv:,.2f} - total_project_cost {total_project_cost:,.2f} "
                    f"= {expected_profit:,.2f}; "
                    f"ROI = {expected_profit:,.2f} / {total_invested_cash:,.2f} "
                    f"= {expected_roi:.2%}"
                ),
            ))

            # Step 7: Max offer price — algebraic non-circular (Spec 7.5)
            selling_costs_arv = round(arv * settings.SELLING_COST_PCT, 2)
            target_profit = round(arv * settings.TARGET_PROFIT_PCT, 2)

            financing_cost_factor = (1 - settings.DOWN_PAYMENT_PCT) * (
                settings.ORIGINATION_POINTS_PCT
                + (settings.HARD_MONEY_RATE_MONTHLY * settings.HOLDING_MONTHS)
            )
            denominator = 1 + settings.CLOSING_COST_PCT + financing_cost_factor

            numerator = (
                arv
                - total_rehab
                - risk_adjusted_contingency
                - selling_costs_arv
                - target_profit
            )

            if denominator:
                max_offer_price = round(numerator / denominator, 2)
            else:
                logger.warning(
                    "max_offer_price denominator was zero for lead %s — defaulting to $0",
                    state.property_id,
                )
                max_offer_price = 0.0
            state.max_offer_price = max_offer_price

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="max_offer_price",
                value_before=None,
                value_after=max_offer_price,
                rationale=(
                    f"Algebraic formula (Spec 7.5): "
                    f"numerator = {numerator:,.2f}; "
                    f"denominator = {denominator:.4f}; "
                    f"max_offer = {max_offer_price:,.2f}"
                ),
            ))

            # Step 8: Rule of 70 sanity check (Spec 7.6)
            rule_of_70_max = round((arv * 0.70) - total_rehab, 2)
            state.rule_of_70_max = rule_of_70_max

            if rule_of_70_max > 0 and max_offer_price > 0:
                divergence = round(
                    abs(max_offer_price - rule_of_70_max) / rule_of_70_max, 4
                )
            else:
                divergence = None
            state.max_offer_divergence_pct = divergence

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="rule_of_70_max",
                value_before=None,
                value_after=rule_of_70_max,
                rationale=(
                    f"rule_of_70 = (arv {arv:,.2f} x 0.70) - rehab {total_rehab:,.2f} "
                    f"= {rule_of_70_max:,.2f}; "
                    f"divergence = {f'{divergence:.2%}' if divergence is not None else 'N/A'}"
                ),
            ))

            # Step 9: Execution score and data confidence score
            execution_score, exec_rationale = compute_execution_score(state)
            state.execution_score = execution_score
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="execution_score",
                value_before=None,
                value_after=execution_score,
                rationale=exec_rationale,
            ))

            data_conf_score, data_conf_rationale = compute_data_confidence_score(state)
            state.data_confidence_score = data_conf_score
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="data_confidence_score",
                value_before=None,
                value_after=data_conf_score,
                rationale=data_conf_rationale,
            ))

            # Step 10: Final rank score (Spec 8.4)
            roi_normalized = min(
                100.0,
                (expected_roi / settings.RANKING_MAX_ROI_CEILING) * 100
            )
            distress = state.distress_score or 0.0

            raw_rank = (
                (roi_normalized * settings.RANK_WEIGHT_ROI)
                + (distress * settings.RANK_WEIGHT_DISTRESS)
                + (execution_score * settings.RANK_WEIGHT_EXECUTION)
                + (data_conf_score * settings.RANK_WEIGHT_DATA_CONFIDENCE)
            )

            # Static penalties per Spec 8.4
            penalties: list[str] = []
            penalty_total = 0.0

            if state.purchase_price_method == "proxy_arv_factor":
                penalty_total -= 10.0
                penalties.append("proxy_pricing -10")

            if state.arv_confidence == "medium":
                penalty_total -= 5.0
                penalties.append("arv_medium -5")

            if state.capex_confidence == "low":
                penalty_total -= 15.0
                penalties.append("capex_low -15")

            if divergence is not None and divergence > settings.MAX_OFFER_DIVERGENCE_ALERT_PCT:
                penalty_total -= 5.0
                penalties.append("70_rule_divergence -5")

            final_rank_score = round(max(0.0, min(100.0, raw_rank + penalty_total)), 2)
            state.final_rank_score = final_rank_score

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="final_rank_score",
                value_before=None,
                value_after=final_rank_score,
                rationale=(
                    f"Weighted: roi_norm {roi_normalized:.1f}x{settings.RANK_WEIGHT_ROI} "
                    f"+ distress {distress:.1f}x{settings.RANK_WEIGHT_DISTRESS} "
                    f"+ exec {execution_score}x{settings.RANK_WEIGHT_EXECUTION} "
                    f"+ data_conf {data_conf_score}x{settings.RANK_WEIGHT_DATA_CONFIDENCE} "
                    f"= {raw_rank:.2f}; "
                    f"penalties [{', '.join(penalties) or 'none'}] {penalty_total:+.1f} "
                    f"-> {final_rank_score}"
                ),
            ))

            # Step 11: Routing — build reason_codes, then assign recommendation
            reason_codes: list[str] = []
            manual_reasons: list[str] = []

            # Reject conditions
            if max_offer_price <= 0:
                reason_codes.append("NEGATIVE_OR_ZERO_MAX_OFFER")
            if expected_roi < 0.10:
                reason_codes.append("ROI_BELOW_FLOOR")

            # Manual review conditions (stackable)
            if state.arv_confidence == "low":
                manual_reasons.append("INSUFFICIENT_COMPS")
            if capex_conf == "low":
                manual_reasons.append("LOW_CAPEX_CONFIDENCE")
            if (state.purchase_price_method == "proxy_arv_factor"
                    and not settings.ALLOW_PROXY_APPROVAL):
                manual_reasons.append("PROXY_PRICE_ONLY")
            if 0.10 <= expected_roi < 0.15:
                manual_reasons.append("ROI_NEAR_THRESHOLD")
            if divergence is not None and divergence > settings.MAX_OFFER_DIVERGENCE_ALERT_PCT:
                manual_reasons.append("MAX_OFFER_DIVERGES_FROM_70_RULE")

            # Apply routing precedence
            if reason_codes:  # reject
                recommendation = "rejected"
                reason_codes_final = reason_codes
                summary = (
                    f"Rejected: {', '.join(reason_codes)}. "
                    f"ROI={expected_roi:.2%}, max_offer={max_offer_price:,.0f}"
                )
                next_action = "Do not pursue — below minimum return threshold"

            elif manual_reasons:  # manual review
                recommendation = "manual_review"
                reason_codes_final = manual_reasons
                summary = (
                    f"Manual review required: {', '.join(manual_reasons)}. "
                    f"ROI={expected_roi:.2%}, max_offer={max_offer_price:,.0f}"
                )
                next_action = "Review flagged concerns before proceeding with offer"

            else:  # approved
                recommendation = "approved"
                reason_codes_final = ["MEETS_THRESHOLD"]
                summary = (
                    f"Approved: ROI={expected_roi:.2%} meets target, "
                    f"confidence bands adequate, "
                    f"max_offer={max_offer_price:,.0f}"
                )
                next_action = (
                    "Submit for offer review and engage title company"
                )

            state.decision = AcquisitionDecision(
                recommendation=recommendation,  # type: ignore[arg-type]
                reason_codes=reason_codes_final,
                summary=summary,
                next_action=next_action,
            )

            # Propagate manual_review_reasons to state list
            for r in manual_reasons + reason_codes:
                if r not in state.manual_review_reasons:
                    state.manual_review_reasons.append(r)

            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="decision",
                value_before=None,
                value_after=recommendation,
                rationale=(
                    f"Routing: {recommendation} — {', '.join(reason_codes_final)}"
                ),
            ))

            # Step 12: Advance stage. Rejected deals skip CRM publishing
            # entirely and go straight to the 'rejected' terminal stage so
            # Node 9 never sees them and dashboards can count outcomes cleanly.
            prior_stage = state.pipeline_stage
            next_stage = "rejected" if recommendation == "rejected" else "underwritten"
            audit_entries.append(AuditEntry(
                timestamp=now_iso,
                node=NODE_NAME,
                field_mutated="pipeline_stage",
                value_before=prior_stage,
                value_after=next_stage,
                rationale=(
                    f"Underwriting complete — decision={recommendation}, "
                    f"advancing to '{next_stage}'"
                ),
            ))

            state.pipeline_stage = next_stage  # type: ignore[assignment]
            state.data_sources.append(NODE_NAME)
            state.audit_log.extend(audit_entries)
            upsert_lead(state, db_path)

            metrics[recommendation] += 1  # type: ignore[literal-required]

            logger.info(
                "node_08_underwriting: underwritten",
                extra={
                    "property_id": state.property_id,
                    "apn": state.apn,
                    "recommendation": recommendation,
                    "expected_roi": expected_roi,
                    "max_offer_price": max_offer_price,
                    "final_rank_score": final_rank_score,
                    "reason_codes": reason_codes_final,
                },
            )

        except Exception as exc:
            metrics["errors"] += 1
            now_iso = utc_now_iso()

            _fail_lead(
                state, audit_entries,
                error_code="UNDERWRITING_INPUT_MISSING",
                message=f"property_id={state.property_id}: {exc}",
                review_reason="UNDERWRITING_INPUT_MISSING",
                rationale=f"Unexpected exception during underwriting: {exc}",
                now_iso=now_iso,
                db_path=db_path,
            )
            logger.exception(
                "node_08_underwriting: underwriting failed",
                extra={"property_id": state.property_id},
            )

    logger.info(
        "node_08_underwriting complete",
        extra={"run_id": run_id, "metrics": metrics},
    )
    return metrics


if __name__ == "__main__":
    from core.database import init_db

    logging.basicConfig(level=logging.INFO)
    init_db()
    print(run_underwriting("manual-underwriting-check"))
