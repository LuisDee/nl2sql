"""Tests for YAML lru_cache in catalog_loader."""

from nl2sql_agent.catalog_loader import clear_yaml_cache, load_yaml


class TestYamlCache:
    def setup_method(self):
        clear_yaml_cache()

    def test_second_call_uses_cache(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("table:\n  name: markettrade\n")

        result1 = load_yaml(yaml_file)
        result2 = load_yaml(yaml_file)

        assert result1 == result2
        assert result1 is result2  # Same object reference — cache hit

    def test_different_paths_cached_independently(self, tmp_path):
        file_a = tmp_path / "a.yaml"
        file_b = tmp_path / "b.yaml"
        file_a.write_text("table:\n  name: alpha\n")
        file_b.write_text("table:\n  name: beta\n")

        result_a = load_yaml(file_a)
        result_b = load_yaml(file_b)

        assert result_a["table"]["name"] == "alpha"
        assert result_b["table"]["name"] == "beta"

    def test_clear_cache_forces_reread(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("table:\n  name: original\n")

        result1 = load_yaml(yaml_file)
        assert result1["table"]["name"] == "original"

        # Modify file on disk
        yaml_file.write_text("table:\n  name: updated\n")

        # Still cached
        result2 = load_yaml(yaml_file)
        assert result2["table"]["name"] == "original"

        # Clear cache → re-reads from disk
        clear_yaml_cache()
        result3 = load_yaml(yaml_file)
        assert result3["table"]["name"] == "updated"

    def test_cache_info_shows_hits(self, tmp_path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text("value: 42\n")

        load_yaml(yaml_file)
        load_yaml(yaml_file)
        load_yaml(yaml_file)

        info = load_yaml.cache_info()
        assert info.hits == 2
        assert info.misses == 1
