"""Schema compatibility checks between versions.

Two schemas are backward-compatible if:
1. No fields were removed.
2. No field types were narrowed (e.g. string → integer).
3. New fields were added (additive change).

Breaking changes require a new collection or explicit migration.
The system never auto-migrates data between schema versions.
"""

from __future__ import annotations

from ameoba.domain.schema import SchemaCompatibility


def check_compatibility(
    old_schema: dict,
    new_schema: dict,
) -> SchemaCompatibility:
    """Compare two JSON Schema dicts and return a compatibility verdict.

    Args:
        old_schema: The previous schema version.
        new_schema: The candidate new schema version.

    Returns:
        ``SchemaCompatibility`` enum value.
    """
    if old_schema == new_schema:
        return SchemaCompatibility.IDENTICAL

    old_props = old_schema.get("properties", {})
    new_props = new_schema.get("properties", {})

    removed_fields = set(old_props) - set(new_props)
    if removed_fields:
        return SchemaCompatibility.BREAKING

    # Check for type narrowing on existing fields
    for field, old_field_schema in old_props.items():
        if field not in new_props:
            continue
        new_field_schema = new_props[field]
        if _is_type_narrowed(old_field_schema, new_field_schema):
            return SchemaCompatibility.BREAKING

    # All existing fields present and not narrowed → backward compatible
    return SchemaCompatibility.BACKWARD_COMPATIBLE


def _is_type_narrowed(old: dict, new: dict) -> bool:
    """Return True if the new schema is narrower (breaking change)."""
    old_type = old.get("type", "string")
    new_type = new.get("type", "string")

    if old_type == new_type:
        # Recurse into object properties
        if old_type == "object":
            old_props = old.get("properties", {})
            new_props = new.get("properties", {})
            for field in old_props:
                if field not in new_props:
                    return True  # Field removed in nested object
                if _is_type_narrowed(old_props[field], new_props[field]):
                    return True
        return False

    # Widening is ok; narrowing is breaking
    _WIDENING_ALLOWED = {
        ("integer", "number"),
        ("integer", "string"),
        ("number", "string"),
        ("boolean", "integer"),
        ("boolean", "string"),
        ("null", "string"),
        ("null", "integer"),
        ("null", "number"),
        ("null", "boolean"),
        ("null", "array"),
        ("null", "object"),
    }
    return (old_type, new_type) not in _WIDENING_ALLOWED
