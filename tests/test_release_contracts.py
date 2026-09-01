import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_required_release_trees_exist():
    for relative in ("core/src/main", "docs", "reference", "integration/zingg-0.7.0-overlay"):
        assert (ROOT / relative).is_dir(), relative


def test_serverless_launcher_does_not_stop_managed_session():
    source = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "spark.stop()" not in source


def test_managed_phase_summary_is_attempted_once_on_success_and_failure():
    source = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "var phaseSummaryEmitted = false" in source
    assert "def emitOrdinaryPhaseSummary()" in source
    assert "try emitOrdinaryPhaseSummary()" in source
    assert "NATIVE_PHASE_SUMMARY_WARNING" in source


def test_blocking_probe_covers_supported_families_and_unsupported_fail_closed():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessBlockingDifferentialProbe.scala").read_text(encoding="utf-8")
    for name in ("SparkFirstChars", "SparkLastChars", "SparkLastWord", "last4Chars"):
        assert name in probe
    assert "must fail closed for unsupported last4Chars" in probe


def test_source_context_gate_pins_reference_and_overlay_digests():
    lock = (ROOT / "reference/zingg-0.7.0-spark4.lock").read_text()
    checker = (ROOT / "scripts/check-source-context.ps1").read_text()
    assert "referenceTreeDigest=" in lock
    assert "overlayTreeDigest=" in lock
    assert "Reference tree drift" in checker
    assert "Overlay tree drift" in checker
    assert "overlayFiles" in checker


def test_native_training_does_not_force_single_partition():
    source = (ROOT / "integration/zingg-0.7.0-overlay/spark/core/src/main/java/zingg/spark/core/model/SparkModel.java").read_text()
    assert ".coalesce(1)" not in source
    graph = (ROOT / "core/src/main/scala/ai/zingg/native/NativeGraph.scala").read_text()
    assert ".coalesce(1)" not in graph
    blocking = (ROOT / "integration/zingg-0.7.0-overlay/spark/core/src/main/java/zingg/spark/core/util/SparkBlockingTreeUtil.java").read_text()
    assert ".coalesce(1)" not in blocking


def test_managed_session_and_lazy_read_contracts_are_explicit():
    client = (ROOT / "integration/zingg-0.7.0-overlay/spark/client/src/main/java/zingg/spark/client/SparkClient.java").read_text()
    reader = (ROOT / "integration/zingg-0.7.0-overlay/spark/client/src/main/java/zingg/spark/client/util/SparkDFReader.java").read_text()
    assert "refusing to create a detached session" in client
    assert "loaded.limit(1).count()" not in reader


def test_managed_cache_paths_are_guarded_or_removed_from_native_training():
    graph = (ROOT / "integration/zingg-0.7.0-overlay/spark/core/src/main/java/zingg/spark/core/util/SparkGraphUtil.java").read_text()
    model = (ROOT / "integration/zingg-0.7.0-overlay/spark/core/src/main/java/zingg/spark/core/model/SparkModel.java").read_text()
    assert ".cache(" not in graph
    assert "return union.cache();" not in model


def test_source_boundary_checker_is_not_vacuous():
    source = (ROOT / "scripts/check-source-boundaries.py").read_text()
    assert "required production source tree is missing" in source


def test_classic_strict_activation_does_not_swallow_bridge_failures():
    adapter = (ROOT / "python/src/zingg_native/adapter.py").read_text()
    assert "BackendUnavailableError" in adapter
    assert 'if cfg.mode == "STRICT"' in adapter
    assert 'Unable to activate STRICT native mode' in adapter
    assert 'except Exception:\n            pass' not in adapter


def test_validation_docs_distinguish_runtime_evidence_from_certification():
    testing = " ".join((ROOT / "docs/testing.md").read_text().split())
    compatibility = " ".join((ROOT / "docs/compatibility.md").read_text().split())
    assert "20-feature/1,770-term production training path" in testing
    assert "Complete phase-level oracle parity" in testing
    assert "operator attribution unverified" in compatibility
    assert "Dedicated execution remain unvalidated" in compatibility


def test_serverless_profile_versions():
    pom = (ROOT / "pom.xml").read_text()
    assert "<scala.version>2.13.16</scala.version>" in pom
    assert "<databricks.connect.version>18.0.0</databricks.connect.version>" in pom
    assert "<maven.compiler.release>17</maven.compiler.release>" in pom


def test_serverless_dependency_boundary_rejects_forbidden_transitives():
    checker = (ROOT / "scripts/check-serverless-dependencies.ps1").read_text()
    for forbidden in ("graphframes", "_2.12", "spark-connect-server-plugin", "zingg-native-connect"):
        assert forbidden in checker
    assert "2.13.16" in checker and ":provided" in checker
    assert "18.0.0" in checker and ":provided" in checker


def test_bundle_has_explicit_serverless_target_and_serverless_compute():
    bundle = (ROOT / "databricks.yml").read_text()
    resources = (ROOT / "resources/serverless-zingg-native.yml").read_text()
    assert "  serverless:" in bundle
    assert 'environment_version: "5"' in resources
    assert "spark_version" not in resources
    assert "node_type_id" not in resources


def test_serverless_resources_use_bundle_workspace_and_volume_variables():
    for path in (ROOT / "resources").glob("serverless-*.yml"):
        text = path.read_text()
        assert "/Workspace/Shared/zingg-native/e2e" not in text, path.name
        assert "/Volumes/sda_dev/default/zingg_native_e2e_volume" not in text, path.name
    bundle = (ROOT / "databricks.yml").read_text()
    assert "workspace_fixture_root:" in bundle
    assert "catalog:" in bundle and "schema:" in bundle and "volume:" in bundle


def test_phase_contract_includes_update_label():
    contract = (ROOT / "reference/zingg-0.7-phase-contract.json").read_text()
    checker = (ROOT / "scripts/check-phase-contract.ps1").read_text()
    assert '"id": "updateLabel"' in contract
    assert "'updateLabel'" in checker


def test_release_set_and_license_contract_are_explicit():
    bundle = (ROOT / "databricks.yml").read_text()
    resources = (ROOT / "resources/serverless-zingg-native.yml").read_text()
    publisher = (ROOT / "scripts/publish-databricks-serverless.ps1").read_text()
    assert "${var.release_id}" in bundle
    assert "manifest.json" in publisher
    assert "${var.license_path}" in bundle + resources
    assert 'default: /Workspace/Shared/zingg-native/e2e/LICENSE' in bundle
    assert "/e2e/license.txt" not in resources


def test_serverless_rejects_unimplemented_legacy_modes_at_launcher_boundary():
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert 'case "OFF" | "AUDIT"' in launcher
    assert "use REWRITE or STRICT" in launcher


def test_fixture_publisher_covers_bundle_asset_names_without_shipping_license():
    publisher = (ROOT / "scripts/publish-serverless-fixtures.ps1").read_text()
    for name in ("config-volume.json", "config-minimal.json", "config-tiny-marked.json", "prepare-minimal-parquet.py", "seed-minimal-labels.py"):
        assert name in publisher
    assert "LICENSE" in publisher
    assert "Catalog" in publisher and "Schema" in publisher and "Volume" in publisher
    assert "sourceVolumeRoot" in publisher and "sourceFixtureRoot" in publisher


def test_patched_build_is_pinned_to_the_checked_out_reference_commit():
    lock = (ROOT / "reference/zingg-0.7.0-spark4.lock").read_text()
    builder = (ROOT / "scripts/build-patched-zingg.ps1").read_text()
    assert "21f54e9b4693d49d6f26b2851853b140951a7502" in lock
    assert "sparkProfile=databricks-serverless-env5" in lock
    assert "sparkVersion=4.1.0" in lock
    assert "scalaVersion=2.13.16" in lock
    assert "javaRelease=17" in lock
    assert "does not match lock commit" in builder
    assert "not aligned with Databricks Serverless environment 5" in builder
    assert "managedInvocation() && success" in builder
    assert "Zingg processing failed in managed invocation" in builder


def test_quick_model_probe_batches_terms_without_changing_the_gradient():
    model = (ROOT / "core/src/main/scala/ai/zingg/native/NativeModel.scala").read_text()
    assert "if (quickProbe) math.max(1, terms.length)" in model


def test_bounded_full_shape_probe_preserves_degree_three_and_is_not_production_default():
    model = (ROOT / "core/src/main/scala/ai/zingg/native/NativeModel.scala").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    resource = (ROOT / "resources/serverless-zingg-native.yml").read_text()
    assert "boundedProbe" in model
    assert "quickProbe || boundedProbe" in model
    assert "NativeModelBoundedProbeFlag" in launcher
    assert "--native-model-bounded-probe" in resource
    production = (ROOT / "resources/serverless-production-validation.yml").read_text()
    assert "--native-model-bounded-probe" not in production


def test_fuzzy_diagnostic_exercises_full_degree_path():
    resource = (ROOT / "resources/serverless-one-fuzzy-probe.yml").read_text()
    assert "--native-model-bounded-probe" in resource
    assert "--native-model-quick-probe" not in resource


def test_fuzzy_action_probe_is_separate_from_model_training():
    resource = (ROOT / "resources/serverless-fuzzy-action-probe.yml").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "--native-fuzzy-action-probe" in resource
    assert "NativeFuzzyActionProbeFlag" in launcher
    assert "ServerlessFuzzyActionProbe.run" in launcher
    evidence = json.loads((ROOT / "docs/evidence/databricks-serverless-v5.json").read_text())
    assert evidence["serverlessFuzzyActionModelIntegrationProbe"]["resultState"] == "SUCCESS"


def test_fuzzy_scale_probe_has_model_only_diagnostic_mode():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessFuzzyActionProbe.scala").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "zingg.native.fuzzy.modelOnly" in probe
    assert "--native-fuzzy-model-only" in launcher
    evidence = json.loads((ROOT / "docs/evidence/databricks-serverless-v5.json").read_text())
    assert evidence["serverlessFuzzyActionModelOnlyScale100Probe"]["resultState"] == "SUCCESS"


def test_adapter_only_fuzzy_flags_are_consumed_before_delegate():
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert 'case NativeModelBoundedProbeFlag => System.setProperty' in launcher
    assert 'case "--native-fuzzy-action-rows" => System.setProperty' in launcher
    assert 'case "--native-fuzzy-model-only" => System.setProperty' in launcher
    assert 'case "--native-fuzzy-action-rule" => System.setProperty' in launcher


def test_fuzzy_action_probe_can_isolate_jaro_and_affine_rules():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessFuzzyActionProbe.scala").read_text()
    assert 'zingg.native.fuzzy.action.rule' in probe
    assert 'JaroWinklerFunction' in probe and 'AffineGapSimilarityFunction' in probe
    assert 'stages.mkString' in probe


def test_serverless_negative_probes_are_declared():
    probes = (ROOT / "resources/serverless-failure-probes.yml").read_text()
    assert "unknown_operation" in probes
    assert "rewrite.similarity.exact" in probes
    assert "environment_version: \"5\"" in probes


def test_blocking_differential_probe_compares_candidate_sets():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessBlockingDifferentialProbe.scala").read_text(encoding="utf-8")
    resource = (ROOT / "resources/serverless-blocking-differential-probe.yml").read_text()
    assert "candidate-pair" in probe
    assert "nativePairs == legacyPairs" in probe
    assert "--native-blocking-differential-probe" in resource


def test_row_id_probe_checks_opaque_uniqueness_contract():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessRowIdProbe.scala").read_text()
    resource = (ROOT / "resources/serverless-row-id-probe.yml").read_text()
    assert "ids.distinct.length == ids.length" in probe
    assert "publishedClusterId=false" in probe
    assert "--native-row-id-probe" in resource


def test_cross_job_model_persistence_is_two_serverless_jobs():
    resource = (ROOT / "resources/serverless-model-cross-job.yml").read_text()
    bundle = (ROOT / "databricks.yml").read_text()
    assert "zingg_native_serverless_model_cross_job_train" in resource
    assert "zingg_native_serverless_model_cross_job_load" in resource
    assert "--native-model-load-probe" in resource
    assert "${var.model_probe_path}" in resource
    assert resource.count("--native-model-probe-features") == 2
    assert '"2"' in resource
    assert "resources/serverless-model-cross-job.yml" in bundle


def test_row_id_rewrite_uses_public_unique_id_expression():
    source = (ROOT / "integration/zingg-0.7.0-overlay/spark/client/src/main/scala/reifier/scala/DFUtil.scala").read_text()
    assert "df.withColumn(name, monotonically_increasing_id())" in source
    assert "zipWithUniqueId" not in source


def test_row_id_contract_is_explicit_about_scope_and_stability():
    contract = " ".join((ROOT / "docs/semantic-contracts.md").read_text().split())
    assert "opaque, non-null, unique row key" in contract
    assert "not required to be stable across separate actions or jobs" in contract
    assert "not a published Zingg `z_zid`" in contract


def test_python_surface_does_not_ship_a_second_semantic_engine():
    backend = (ROOT / "python/src/zingg_native/backend/base.py").read_text()
    assert "NativeOperationProvider" in backend
    assert "from ..expressions import" not in backend
    assert not (ROOT / "python/src/zingg_native/expressions.py").exists()


def test_patched_build_materializes_reference_archive_before_extracting():
    script = (ROOT / "scripts/build-patched-zingg.ps1").read_text()
    assert "git -C $Reference archive --format=tar --output=$archivePath HEAD" in script
    assert "git -C $Reference archive --format=tar HEAD | tar -xf -" not in script
    assert "--date=2000-01-01T00:00:00Z" in script


def test_serverless_patched_assembly_removes_unsupported_cache_call():
    script = (ROOT / "scripts/build-patched-zingg.ps1").read_text()
    assert "unsupported cache call entirely" in script
    assert 'return new SparkFrame(df);' in script


def test_ordinary_feature_materialization_uses_one_stable_scalar_boundary():
    source = (ROOT / "integration/zingg-0.7.0-overlay/spark/core/src/main/java/zingg/spark/core/model/SparkModel.java").read_text()
    assert 'String assembledPath = path + "/assembled"' in source
    assert "scalar feature frame" in source
    assert "assembled.join(chunk, \"_native_row_id\")" not in source


def test_registered_hash_family_has_a_public_native_rule():
    architecture = (ROOT / "core/src/main/scala/ai/zingg/native/RewriteArchitecture.scala").read_text()
    rules = (ROOT / "core/src/main/scala/ai/zingg/native/PublicRewriteRules.scala").read_text()
    block = architecture.split('private val hashNames = Seq(', 1)[1].split(')\n  val preprocessNames', 1)[0]
    names = re.findall(r'"([A-Za-z0-9]+)"', block)
    assert len(names) >= 50
    assert 'private val hashes:Seq[RewriteRule]=Seq(' in rules
    assert 'rewrite.blocking.first${n}' in rules
    assert 'rewrite.blocking.last${n}' in rules
    assert 'rewrite.blocking.round' in rules


def test_serverless_connect_does_not_pull_malformed_dbutils_bridge():
    pom = (ROOT / "core/pom.xml").read_text()
    assert "databricks-dbutils-scala_2.13" in pom
    assert "<exclusion>" in pom
    assert pom.index("databricks-dbutils-scala_2.13") < pom.index("</exclusions>")


def test_release_publisher_verifies_uploaded_checksums():
    script = (ROOT / "scripts/publish-databricks-serverless.ps1").read_text()
    assert "workspace mkdirs $releaseRoot" in script
    assert "workspace export" in script
    assert "Remote checksum mismatch" in script


def test_single_serverless_release_pipeline_orders_build_scan_publish_and_bundle_validation():
    script = (ROOT / "scripts/release-serverless.ps1").read_text()
    for needle in ("build.ps1", "build-patched-zingg.ps1", "check-serverless-bytecode.py", "publish-serverless-fixtures.ps1", "publish-databricks-serverless.ps1", "bundle", "validate"):
        assert needle in script
    assert "-Profile $Profile" in script


def test_full_training_job_classes_are_explicitly_separated():
    diagnostic = (ROOT / "resources/serverless-full-parquet-diagnostic.yml").read_text()
    production = (ROOT / "resources/serverless-production-validation.yml").read_text()
    assert "--native-model-max-iter" in diagnostic
    assert "--native-model-quick-probe" not in diagnostic
    assert "production-validation" in production
    assert "performance-soak" in production
    # Production/soak jobs intentionally rely on NativeModel's convergence
    # default and must not inherit a bounded diagnostic iteration override.
    assert production.count("--native-model-max-iter") == 0
    assert production.count("--native-model-quick-probe") == 0
    assert "max_retries: 2" in production


def test_base_materialization_does_not_assume_a_term_array_sidecar():
    model = (ROOT / "core/src/main/scala/ai/zingg/native/NativeModel.scala").read_text()
    assert 'sys.props.contains("zingg.native.model.termArrayPath")' in model
    assert 'sys.props.contains("zingg.native.model.materializePath") && terms.length > 164' not in model
    assert "case Some(_) => normalizedInput" in model




def test_evidence_requires_cross_job_persistence_pair():
    import json

    evidence = json.loads((ROOT / "docs/evidence/databricks-serverless-v5.json").read_text())
    pair = evidence["currentReleaseCrossJobPersistence"]
    assert pair["train"]["resultState"] == "SUCCESS"
    assert pair["reloadAndMatch"]["resultState"] == "SUCCESS"
    assert pair["train"]["runId"] != pair["reloadAndMatch"]["runId"]
    assert pair["train"]["taskRunId"] != pair["reloadAndMatch"]["taskRunId"]


def test_model_probe_exposes_explicit_cross_job_load_contract():
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessModelProbe.scala").read_text()
    assert "--native-model-load-probe" in launcher
    assert "--native-model-probe-path" in launcher
    assert "loadAndPredict" in probe
    assert "NATIVE_MODEL_PROBE_LOAD_PASS" in probe
    assert "predictionFingerprint" in probe
    assert "probe-predictions" in probe
    assert "metadata=true predictions=true" in probe


def test_model_probe_checks_persisted_contract_and_prediction_consistency():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessModelProbe.scala").read_text()
    assert 'path.stripSuffix("/")}/_zingg_native_model_v1' in probe
    assert 'savedFeatures == featureColumns.indices.map' in probe
    assert 'savedCoefficients.size == expectedTerms' in probe
    assert 'probe.features' in probe
    assert 'polynomialOrdering") == "spark-polynomial-expansion-order-v1"' in probe
    assert 'regParam") == 0.0001d' in probe
    assert 'threshold") == 0.40d' in probe
    assert 'Probability/raw_prediction' in probe
    assert 'prediction <> CASE WHEN score > 0.40' in probe


def test_corrupt_model_sidecar_fails_closed_with_rule_identifier():
    model = (ROOT / "core/src/main/scala/ai/zingg/native/NativeModel.scala").read_text()
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessModelCorruptionProbe.scala").read_text()
    resource = (ROOT / "resources/serverless-model-failure-probe.yml").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "model.nativePersistence.load rejected a corrupt or incompatible sidecar" in model
    assert "NATIVE_MODEL_CORRUPTION_EXPECTED_FAILURE" in probe
    assert "--native-model-corruption-probe" in resource
    assert "ServerlessModelCorruptionProbe.run" in launcher


def test_model_validation_jobs_have_bounded_serverless_timeouts():
    for name in ("serverless-model-cross-job.yml", "serverless-model-failure-probe.yml", "serverless-model-semantic-probe.yml"):
        resource = (ROOT / "resources" / name).read_text()
        assert "timeout_seconds:" in resource


def test_native_transient_materialization_is_uuid_scoped_and_safe_to_retry():
    lifecycle = (ROOT / "core/src/main/scala/ai/zingg/native/NativeMaterializationLifecycle.scala").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "UUID.fromString(safeRunId)" in lifecycle
    assert ".native-transient" in lifecycle
    assert "walkFileTree" in lifecycle
    assert "NOFOLLOW_LINKS" in lifecycle
    assert "toRealPath" in lifecycle
    assert "isSymbolicLink" in lifecycle
    assert "rejectSymlinkAncestors" in lifecycle
    assert "cleaned.getAndSet(true)" not in lifecycle
    assert "NativeMaterializationLifecycle.cleanup(root)" in launcher
    assert "spark.stop()" not in launcher


def test_graph_nonconvergence_fails_closed_in_every_mode():
    graph = (ROOT / "core/src/main/scala/ai/zingg/native/NativeGraph.scala").read_text()
    assert "if (!converged)" in graph
    assert "!converged && context.mode == NativeExecutionMode.STRICT" not in graph
    assert "Connected-components did not converge" in graph


def test_launcher_refuses_unmanaged_materialization_fallback():
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "requires --zinggDir" in launcher
    assert "/Volumes/sda_dev/default/zingg_native_e2e_volume/.native-transient/base" not in launcher


def test_launcher_rejects_graph_materialization_outside_run_root():
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "explicit graph materialization must be beneath the native run root" in launcher
    assert "explicit graph materialization requires --zinggDir" in launcher


def test_graph_materialization_is_under_the_run_scoped_transient_root():
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert 'orElse(runRoot.map(v => s"$v/graph"))' in launcher
    assert 's"${path.stripSuffix("/")}/$runId"' in launcher


def test_materialization_recovery_probe_has_expected_failure_and_followup_job():
    resource = (ROOT / "resources/serverless-materialization-recovery.yml").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "zingg_native_serverless_materialization_failure_probe" in resource
    assert "zingg_native_serverless_materialization_recovery_probe" in resource
    assert "--native-materialization-failure-probe" in resource
    assert "--native-materialization-recovery-probe" in resource
    assert "ServerlessMaterializationRecoveryProbe.leaveFailedSentinel" in launcher
    assert "ServerlessMaterializationRecoveryProbe.verifyRecovered" in launcher


def test_numeric_differential_probe_covers_typed_similarity_boundaries():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessNumericDifferentialProbe.scala").read_text()
    resource = (ROOT / "resources/serverless-numeric-differential-probe.yml").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "IntegerSimilarityFunction" in probe
    assert "LongSimilarityFunction" in probe
    assert "Double.NaN" in probe
    assert "Long.MinValue" in probe
    assert "NATIVE_NUMERIC_DIFFERENTIAL_SUMMARY" in probe
    assert "--native-numeric-differential-probe" in resource
    assert "ServerlessNumericDifferentialProbe.run" in launcher


def test_date_and_array_differential_probe_covers_typed_similarity_boundaries():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessDateArrayDifferentialProbe.scala").read_text()
    resource = (ROOT / "resources/serverless-date-array-differential-probe.yml").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "DateSimilarityFunction" in probe
    assert "ArrayDoubleSimilarityFunction" in probe
    assert "Date.valueOf(\"2000-02-29\")" in probe
    assert "Array(java.lang.Double.valueOf(1.0d)" in probe
    assert "--native-date-array-differential-probe" in resource
    assert "ServerlessDateArrayDifferentialProbe.run" in launcher


def test_stopwords_differential_probe_uses_pinned_udf_oracle():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessStopWordsDifferentialProbe.scala").read_text()
    resource = (ROOT / "resources/serverless-stopwords-differential-probe.yml").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "RemoveStopWordsUDF" in probe
    assert "sand theatre" in probe
    assert "NATIVE_PREPROCESS_DIFFERENTIAL_PASS" in probe
    assert "--native-stopwords-differential-probe" in resource
    assert "ServerlessStopWordsDifferentialProbe.run" in launcher


def test_input_format_probe_uses_ordinary_patched_zingg_reader():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessInputFormatProbe.scala").read_text()
    resource = (ROOT / "resources/serverless-input-format-probe.yml").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "SparkDFReader" in probe
    assert '"parquet"' in probe and '"json"' in probe and '"csv"' in probe
    assert "NATIVE_INPUT_FORMAT_SUMMARY" in probe
    assert "--native-input-format-probe" in resource
    assert "ServerlessInputFormatProbe.run" in launcher


def test_vector_probe_covers_dense_sparse_and_null_structs():
    probe = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/ServerlessVectorProbe.scala").read_text()
    launcher = (ROOT / "serverless-launcher/src/main/scala/ai/zingg/native/launch/DatabricksZinggMain.scala").read_text()
    assert "dense" in probe and "sparse" in probe and "Null vector mismatch" in probe
    assert "NATIVE_VECTOR_DIFFERENTIAL_PASS" in probe
    assert "--native-vector-probe" in launcher
    assert "ServerlessVectorProbe.run" in launcher


def test_native_model_contract_records_production_grid_and_ordering():
    model = (ROOT / "core/src/main/scala/ai/zingg/native/NativeModel.scala").read_text()
    assert "PolynomialOrdering = \"spark-polynomial-expansion-order-v1\"" in model
    for value in ("0.0001d", "0.001d", "0.01d", "0.1d", "1.0d"):
        assert value in model
    for value in ("0.40d", "0.45d", "0.50d", "0.55d"):
        assert value in model
    assert "val MaxIter = 100" in model
    assert "val NumFolds = 2" in model


def test_full_feature_production_evidence_is_successful_but_certification_is_open():
    import json

    evidence = json.loads((ROOT / "docs/evidence/databricks-serverless-v5.json").read_text())
    current = evidence["serverlessProductionValidationCurrentRelease"]
    assert current["resultState"] == "SUCCESS"
    assert current["releaseId"] == "20260823-jaro-lifecycle-final"
    assert current["terms"] == 1770
    diagnostic = evidence["serverlessFullFeatureBoundedDiagnostic"]
    assert diagnostic["resultState"] in {"FAILED", "CANCELED"}
    assert "Scala kernel unresponsive" in diagnostic["diagnostic"]
    assert evidence["certificationStatus"] != "SUCCESS"
    attempt = evidence["serverlessEnv5LockAlignedProductionAttempt"]
    assert attempt["resultState"] == "CANCELED_DIAGNOSTIC"
    assert "model fit-start" in attempt["diagnostic"]
    fuzzy = evidence["serverlessFuzzyRuleIsolation"]
    assert fuzzy["jaro"]["resultState"] == "SUCCESS"
    assert fuzzy["affineGap"]["resultState"] == "SUCCESS"
    assert evidence["serverlessVectorDifferential"]["resultState"] == "SUCCESS"


def test_ci_scans_the_patched_zingg_assembly():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    assert "build-patched-zingg.ps1" in workflow
    assert "dist/zingg-0.7.0-spark4-native.jar" in workflow
    scan = "python scripts/check-serverless-bytecode.py core/target/zingg-native-core_2.13-0.3.0-SNAPSHOT.jar serverless-launcher/target/zingg-native-serverless-launcher_2.13-0.3.0-SNAPSHOT.jar dist/zingg-0.7.0-spark4-native.jar"
    assert scan in workflow


def test_serverless_bytecode_gate_enforces_java17_classfiles():
    checker = (ROOT / "scripts/check-serverless-bytecode.py").read_text()
    assert "MAX_CLASS_MAJOR = 61" in checker
    assert "class-file major" in checker
    assert "struct.unpack('>H'" in checker
