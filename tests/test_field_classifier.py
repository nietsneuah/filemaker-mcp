"""Tests for field_classifier — universal naming rules + override logic."""

from filemaker_mcp.field_classifier import (
    RULES,
    ClassificationResult,
    classify_field,
    classify_table,
    compute_diff,
    enrich_from_annotations,
    read_overrides,
)


class TestRuleDefinitions:
    """Verify the built-in rule set is correct."""

    def test_rules_are_ordered_by_priority(self) -> None:
        priorities = [r.priority for r in RULES]
        assert priorities == sorted(priorities)

    def test_all_rules_have_unique_names(self) -> None:
        names = [r.name for r in RULES]
        assert len(names) == len(set(names))

    def test_default_rule_is_last(self) -> None:
        assert RULES[-1].name == "default"
        assert RULES[-1].confidence == "low"


class TestClassifyField:
    """Test classify_field with various field names."""

    def test_pk_prefix_kp(self) -> None:
        r = classify_field("_kp_CustomerID")
        assert r.field_class == "key"
        assert r.rule_name == "pk_prefix"
        assert r.confidence == "high"

    def test_pk_prefix_pk(self) -> None:
        r = classify_field("_pk_ID")
        assert r.field_class == "key"

    def test_fk_prefix_kf(self) -> None:
        r = classify_field("_kf_LocationID")
        assert r.field_class == "key"
        assert r.rule_name == "fk_prefix"

    def test_fk_prefix_fk(self) -> None:
        r = classify_field("_fk_Parent")
        assert r.field_class == "key"

    def test_speed_calc(self) -> None:
        r = classify_field("_sp_CachedTotal")
        assert r.field_class == "internal"
        assert r.rule_name == "speed_calc"
        assert r.confidence == "high"

    def test_global_g_uppercase(self) -> None:
        r = classify_field("gCurrentDate")
        assert r.field_class == "internal"
        assert r.rule_name == "global_g"
        assert r.confidence == "medium"

    def test_global_g_lowercase_not_matched(self) -> None:
        """'green' should NOT match global_g — requires g + uppercase."""
        r = classify_field("green")
        assert r.field_class == "stored"
        assert r.rule_name == "default"

    def test_global_upper_g_underscore(self) -> None:
        r = classify_field("G_Flag")
        assert r.field_class == "internal"
        assert r.rule_name == "global_G"

    def test_utility_z(self) -> None:
        r = classify_field("zCalcField")
        assert r.field_class == "internal"
        assert r.rule_name == "utility_z"
        assert r.confidence == "medium"

    def test_utility_zz(self) -> None:
        r = classify_field("zzDeveloperOnly")
        assert r.field_class == "internal"
        assert r.rule_name == "utility_z"

    def test_normal_field(self) -> None:
        r = classify_field("Customer_Name")
        assert r.field_class == "stored"
        assert r.rule_name == "default"
        assert r.confidence == "low"

    def test_date_field(self) -> None:
        r = classify_field("ServiceDate")
        assert r.field_class == "stored"

    def test_single_char_g_not_matched(self) -> None:
        """Single char 'g' should not match global_g."""
        r = classify_field("g")
        assert r.field_class == "stored"

    def test_calc_c_prefix(self) -> None:
        """cCustomer_name — lowercase + uppercase = internal via lc_upper."""
        r = classify_field("cCustomer_name")
        assert r.field_class == "internal"
        assert r.rule_name == "lc_upper"
        assert r.confidence == "medium"

    def test_summary_s_prefix(self) -> None:
        """sAmountDriver — lowercase + uppercase = internal."""
        r = classify_field("sAmountDriver")
        assert r.field_class == "internal"
        assert r.rule_name == "lc_upper"

    def test_lc_underscore(self) -> None:
        """s_amount — lowercase + underscore = internal."""
        r = classify_field("s_amount")
        assert r.field_class == "internal"
        assert r.rule_name == "lc_upper"

    def test_x_prefix(self) -> None:
        """xToday — lowercase + uppercase = internal."""
        r = classify_field("xToday")
        assert r.field_class == "internal"

    def test_g_underscore_caught(self) -> None:
        """g_nameFind — g + underscore missed by global_g, caught by lc_upper."""
        r = classify_field("g_nameFind")
        assert r.field_class == "internal"
        assert r.rule_name == "lc_upper"

    def test_z_underscore_caught(self) -> None:
        """z_month — z + underscore missed by utility_z, caught by lc_upper."""
        r = classify_field("z_month")
        assert r.field_class == "internal"
        assert r.rule_name == "lc_upper"

    def test_two_lowercase_not_matched(self) -> None:
        """'customer' — lowercase + lowercase = stored (not special)."""
        r = classify_field("customer")
        assert r.field_class == "stored"
        assert r.rule_name == "default"

    def test_uppercase_start_not_matched(self) -> None:
        """'Customer_Name' starts uppercase — not matched by lc_upper."""
        r = classify_field("Customer_Name")
        assert r.field_class == "stored"


class TestDisabledRules:
    """Test rule disabling via disabled_rules parameter."""

    def test_disable_global_g(self) -> None:
        """With global_g disabled, gCurrentDate falls through to lc_upper."""
        r = classify_field("gCurrentDate", disabled_rules={"global_g"})
        assert r.field_class == "internal"
        assert r.rule_name == "lc_upper"

    def test_disable_utility_z(self) -> None:
        """With utility_z disabled, zCalcField falls through to lc_upper."""
        r = classify_field("zCalcField", disabled_rules={"utility_z"})
        assert r.field_class == "internal"
        assert r.rule_name == "lc_upper"

    def test_disable_multiple(self) -> None:
        """With both g rules disabled, gTest falls through to lc_upper."""
        r = classify_field("gTest", disabled_rules={"global_g", "global_G"})
        assert r.field_class == "internal"
        assert r.rule_name == "lc_upper"

    def test_disable_lc_upper_falls_to_default(self) -> None:
        """With lc_upper also disabled, cField becomes stored."""
        r = classify_field("cField", disabled_rules={"lc_upper"})
        assert r.field_class == "stored"
        assert r.rule_name == "default"

    def test_disable_nonexistent_rule_is_harmless(self) -> None:
        r = classify_field("_kp_ID", disabled_rules={"fake_rule"})
        assert r.field_class == "key"


class TestReadOverrides:
    """Test reading overrides from DDL_CONTEXT dict."""

    def test_read_tenant_wide_rule_override(self) -> None:
        context = {
            ("*", "*", "rule_override"): {"context": '{"global_g": "disabled"}'},
        }
        overrides = read_overrides(context)
        assert "global_g" in overrides.disabled_rules_global

    def test_read_table_rule_override(self) -> None:
        context = {
            ("Orders", "*", "rule_override"): {"context": '{"utility_z": "disabled"}'},
        }
        overrides = read_overrides(context)
        assert "utility_z" in overrides.disabled_rules_by_table.get("Orders", set())

    def test_read_field_class_override(self) -> None:
        """field_class without classification_source = human override."""
        context = {
            ("Orders", "gSpecialPrice", "field_class"): {"context": "stored"},
        }
        overrides = read_overrides(context)
        assert overrides.field_overrides[("Orders", "gSpecialPrice")] == "stored"

    def test_empty_context(self) -> None:
        overrides = read_overrides({})
        assert len(overrides.disabled_rules_global) == 0
        assert len(overrides.field_overrides) == 0

    def test_machine_generated_field_class_ignored(self) -> None:
        """field_class with classification_source companion = machine-generated."""
        context = {
            ("Orders", "Name", "field_class"): {"context": "stored"},
            ("Orders", "Name", "classification_source"): {"context": "rule:default"},
        }
        overrides = read_overrides(context)
        assert len(overrides.field_overrides) == 0

    def test_human_field_class_is_override(self) -> None:
        """field_class WITHOUT classification_source = human override."""
        context = {
            ("Orders", "gSpecial", "field_class"): {"context": "stored"},
        }
        overrides = read_overrides(context)
        assert overrides.field_overrides[("Orders", "gSpecial")] == "stored"


class TestClassifyTable:
    """Test classify_table — classifies all fields in a TableSchema."""

    def test_classify_simple_table(self) -> None:
        schema = {
            "_kp_ID": {"type": "number"},
            "Customer_Name": {"type": "text"},
            "gFlag": {"type": "text"},
            "ServiceDate": {"type": "datetime"},
        }
        results = classify_table("Test", schema)
        assert results["_kp_ID"].field_class == "key"
        assert results["Customer_Name"].field_class == "stored"
        assert results["gFlag"].field_class == "internal"
        assert results["ServiceDate"].field_class == "stored"

    def test_classify_with_field_override(self) -> None:
        """Human override (no classification_source) is respected."""
        schema = {"gSpecialPrice": {"type": "number"}}
        overrides = read_overrides(
            {
                ("Test", "gSpecialPrice", "field_class"): {"context": "stored"},
            }
        )
        results = classify_table("Test", schema, overrides=overrides)
        assert results["gSpecialPrice"].field_class == "stored"
        assert results["gSpecialPrice"].rule_name == "override"

    def test_classify_with_table_rule_disable(self) -> None:
        """Disabling global_g on gPrice — falls through to lc_upper (still internal)."""
        schema = {"gPrice": {"type": "number"}}
        overrides = read_overrides(
            {
                ("Test", "*", "rule_override"): {
                    "context": '{"global_g": "disabled", "lc_upper": "disabled"}'
                },
            }
        )
        results = classify_table("Test", schema, overrides=overrides)
        assert results["gPrice"].field_class == "stored"


class TestComputeDiff:
    """Test diff between current classification and existing DDL_Context."""

    def test_all_new(self) -> None:
        current = {
            ("Orders", "Name"): ClassificationResult("stored", "default", "low"),
        }
        existing: dict[tuple[str, str], str] = {}
        diff = compute_diff(current, existing)
        assert ("Orders", "Name") in diff.new
        assert len(diff.unchanged) == 0

    def test_unchanged(self) -> None:
        current = {
            ("Orders", "Name"): ClassificationResult("stored", "default", "low"),
        }
        existing = {("Orders", "Name"): "stored"}
        diff = compute_diff(current, existing)
        assert ("Orders", "Name") in diff.unchanged
        assert len(diff.new) == 0

    def test_changed(self) -> None:
        current = {
            ("Orders", "Name"): ClassificationResult("stored", "default", "low"),
        }
        existing = {("Orders", "Name"): "internal"}
        diff = compute_diff(current, existing)
        assert ("Orders", "Name") in diff.changed

    def test_removed(self) -> None:
        current: dict[tuple[str, str], ClassificationResult] = {}
        existing = {("Orders", "OldField"): "stored"}
        diff = compute_diff(current, existing)
        assert ("Orders", "OldField") in diff.removed

    def test_mixed(self) -> None:
        current = {
            ("A", "x"): ClassificationResult("stored", "default", "low"),
            ("A", "y"): ClassificationResult("key", "pk_prefix", "high"),
        }
        existing = {
            ("A", "x"): "stored",  # unchanged
            ("A", "z"): "internal",  # removed
        }
        diff = compute_diff(current, existing)
        assert ("A", "x") in diff.unchanged
        assert ("A", "y") in diff.new
        assert ("A", "z") in diff.removed


class TestEnrichFromAnnotations:
    """Test $metadata annotation enrichment for uncertain fields."""

    def test_calculation_becomes_calculated(self) -> None:
        uncertain = {
            ("Orders", "TotalAmount"): ClassificationResult("stored", "default", "low"),
        }
        annotations = {"Orders": {"TotalAmount": {"calculation": True}}}
        enriched = enrich_from_annotations(uncertain, annotations)
        assert enriched[("Orders", "TotalAmount")].field_class == "calculated"
        assert enriched[("Orders", "TotalAmount")].rule_name == "metadata"
        assert enriched[("Orders", "TotalAmount")].confidence == "high"

    def test_summary_becomes_summary(self) -> None:
        uncertain = {
            ("Orders", "GrandTotal"): ClassificationResult("stored", "default", "low"),
        }
        annotations = {"Orders": {"GrandTotal": {"summary": True}}}
        enriched = enrich_from_annotations(uncertain, annotations)
        assert enriched[("Orders", "GrandTotal")].field_class == "summary"

    def test_global_becomes_global(self) -> None:
        uncertain = {
            ("Orders", "AppVersion"): ClassificationResult("stored", "default", "low"),
        }
        annotations = {"Orders": {"AppVersion": {"global_": True}}}
        enriched = enrich_from_annotations(uncertain, annotations)
        assert enriched[("Orders", "AppVersion")].field_class == "global"

    def test_no_annotation_stays_stored(self) -> None:
        uncertain = {
            ("Orders", "Name"): ClassificationResult("stored", "default", "low"),
        }
        annotations: dict = {}
        enriched = enrich_from_annotations(uncertain, annotations)
        assert enriched[("Orders", "Name")].field_class == "stored"
        assert enriched[("Orders", "Name")].confidence == "low"

    def test_multiple_annotations_priority(self) -> None:
        """Calculation takes priority over summary."""
        uncertain = {
            ("Orders", "Mixed"): ClassificationResult("stored", "default", "low"),
        }
        annotations = {"Orders": {"Mixed": {"calculation": True, "summary": True}}}
        enriched = enrich_from_annotations(uncertain, annotations)
        assert enriched[("Orders", "Mixed")].field_class == "calculated"
