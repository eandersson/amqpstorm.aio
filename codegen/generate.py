import keyword
import pathlib
import xml.etree.ElementTree as ElementTree

SPEC_XML = pathlib.Path(__file__).parent / "amqp0-9-1.extended.xml"
OUTPUT_PATH = pathlib.Path(__file__).parent.parent / "amqpstorm_aio" / "spec.py"

_BASE_TYPES = {
    "bit",
    "octet",
    "short",
    "long",
    "longlong",
    "shortstr",
    "longstr",
    "table",
    "timestamp",
}

_PYTHON_TYPE_BY_SPEC_TYPE = {
    "bit": "bool",
    "octet": "int",
    "short": "int",
    "long": "int",
    "longlong": "int",
    "shortstr": "str",
    "longstr": "bytes",
    "table": "dict[str, Any] | None",
    "timestamp": "datetime.datetime | None",
}

_FIXED_RESERVED_ZEROS = {"octet": 1, "short": 2, "long": 4, "longlong": 8, "shortstr": 1}

# Property types for the TypedDict form. Unlike the class, an absent key already means
# "unset", so these are the value types rather than their optional variants.
_MAPPING_TYPE_BY_SPEC_TYPE = {
    "octet": "int",
    "shortstr": "str",
    "table": "dict[str, Any]",
    "timestamp": "datetime.datetime",
}


class Field:
    """One <field> of a method or of the basic content properties."""

    def __init__(self, element: ElementTree.Element, domains: dict[str, str]) -> None:
        self.spec_name = element.get("name", "")
        domain = element.get("domain") or element.get("type") or ""
        self.domain = element.get("domain") or ""
        self.spec_type = domains.get(domain, domain)
        if self.spec_type not in _BASE_TYPES:
            raise ValueError(f"unresolvable field type {domain!r} for {self.spec_name!r}")
        self.reserved = element.get("reserved") == "1" or self.spec_name == "reserved"
        self.asserts = _read_asserts(element)
        self.python_name = self.spec_name.replace("-", "_")
        if keyword.iskeyword(self.python_name):
            self.python_name += "_"


class Method:
    """One <method> of a spec class."""

    def __init__(
        self,
        element: ElementTree.Element,
        class_name: str,
        class_index: int,
        domains: dict[str, str],
    ) -> None:
        self.spec_name = element.get("name", "")
        self.python_name = _camel(self.spec_name)
        self.class_name = class_name
        self.index = (class_index << 16) | int(element.get("index", "0"))
        self.synchronous = element.get("synchronous") == "1"
        self.has_content = element.get("content") == "1"
        self.fields = [Field(f, domains) for f in element.findall("field")]
        self.response_names = [r.get("name", "") for r in element.findall("response")]

    @property
    def qualified_name(self) -> str:
        return f"{_camel(self.class_name)}.{self.python_name}"


class SpecClass:
    """One <class> of the spec."""

    def __init__(self, element: ElementTree.Element, domains: dict[str, str]) -> None:
        self.spec_name = element.get("name", "")
        self.python_name = _camel(self.spec_name)
        self.index = int(element.get("index", "0"))
        self.properties = [Field(f, domains) for f in element.findall("field")]
        self.methods = [
            Method(m, self.spec_name, self.index, domains) for m in element.findall("method")
        ]


def _camel(spec_name: str) -> str:
    return "".join(part.capitalize() for part in spec_name.split("-"))


def _read_asserts(element: ElementTree.Element) -> list[tuple[str, str | None]]:
    """Collect the <assert> constraints declared on a domain or field.

    Relational checks like ``le`` name their bound as a method/field pair rather than
    a value; that referent is preserved as ``method.field``.
    """
    asserts = []
    for element_assert in element.findall("assert"):
        value = element_assert.get("value")
        if value is None:
            method = element_assert.get("method")
            field = element_assert.get("field")
            if method or field:
                value = f"{method or ''}.{field or ''}"
        asserts.append((element_assert.get("check", ""), value))
    return asserts


def _parse(
    path: pathlib.Path,
) -> tuple[list[tuple[str, int, str]], list[SpecClass], dict[str, list[tuple[str, str | None]]]]:
    root = ElementTree.parse(path).getroot()
    domains = {}
    domain_asserts = {}
    for domain in root.findall("domain"):
        domains[domain.get("name", "")] = domain.get("type", "")
        asserts = _read_asserts(domain)
        if asserts:
            domain_asserts[domain.get("name", "")] = asserts
    constants = [
        (c.get("name", ""), int(c.get("value", "0")), c.get("class", ""))
        for c in root.findall("constant")
    ]
    classes = [SpecClass(element, domains) for element in root.findall("class")]
    _apply_recover_erratum(classes)
    return constants, classes, domain_asserts


def _apply_recover_erratum(classes: list[SpecClass]) -> None:
    for spec_class in classes:
        if spec_class.spec_name != "basic":
            continue
        for method in spec_class.methods:
            if method.spec_name == "recover":
                method.synchronous = True
                if "recover-ok" not in method.response_names:
                    method.response_names.append("recover-ok")


def _emit_asserts(
    lines: list[str],
    classes: list[SpecClass],
    domain_asserts: dict[str, list[tuple[str, str | None]]],
) -> None:
    """Emit the spec's <assert> constraints as data.

    amqpstorm_aio.validation compiles these into the checks it applies to outbound
    arguments, so the spec XML -- not hand-written code -- decides what the client
    will send. Brokers are more permissive (RabbitMQ accepts names the regexp
    forbids); holding to the spec keeps messages portable between them.
    """
    lines.append("")
    lines.append("# Constraints declared by <assert> in the spec XML, verbatim.")
    lines.append("DOMAIN_ASSERTS: dict[str, tuple[tuple[str, str | None], ...]] = {")
    for domain_name in sorted(domain_asserts):
        entries = tuple(domain_asserts[domain_name])
        lines.append(f"    {domain_name!r}: {entries!r},")
    lines.append("}")
    lines.append("")
    lines.append("# Per-method field asserts, keyed by the generated method name.")
    lines.append("FIELD_ASSERTS: dict[str, tuple[tuple[str, str, str | None], ...]] = {")
    for spec_class in classes:
        for method in spec_class.methods:
            entries = tuple(
                (field.spec_name, check, value)
                for field in method.fields
                for check, value in field.asserts
            )
            if entries:
                lines.append(f"    {method.qualified_name!r}: {entries!r},")
    lines.append("}")
    lines.append("")
    lines.append("# Which constrained domain each method field draws from.")
    lines.append("FIELD_DOMAINS: dict[str, tuple[tuple[str, str], ...]] = {")
    for spec_class in classes:
        for method in spec_class.methods:
            entries = tuple(
                (field.spec_name, field.domain)
                for field in method.fields
                if field.domain in domain_asserts
            )
            if entries:
                lines.append(f"    {method.qualified_name!r}: {entries!r},")
    lines.append("}")


def _emit_properties_dict(lines: list[str], classes: list[SpecClass]) -> None:
    """Emit the mapping form of the Basic content properties.

    Publishing accepts either this or the Properties class; declaring it as a TypedDict
    means a dict literal with a misspelled or wrongly typed key is a type error rather
    than something only caught at publish time.
    """
    basic = next(spec_class for spec_class in classes if spec_class.spec_name == "basic")
    lines.append("")
    lines.append("")
    lines.append("class PropertiesDict(TypedDict, total=False):")
    lines.append('    """Basic content properties as a mapping; every key is optional."""')
    lines.append("")
    for field in basic.properties:
        if field.reserved:
            continue
        python_type = _MAPPING_TYPE_BY_SPEC_TYPE[field.spec_type]
        lines.append(f"    {field.python_name}: {python_type}")


def _method_index_comment(index: int) -> str:
    return f"0x{index:08X}  # class {index >> 16}, method {index & 0xFFFF}"


def _emit_encode(lines: list[str], method: Method) -> None:
    indent = "        "
    lines.append("")
    lines.append(f"{indent}def encode(self) -> bytearray:")
    lines.append(f'{indent}    """Return the method-frame payload (class + method + arguments)."""')
    prefix = bytearray(method.index.to_bytes(4, "big"))
    fields = list(method.fields)
    while fields and fields[0].reserved and fields[0].spec_type in _FIXED_RESERVED_ZEROS:
        prefix += b"\x00" * _FIXED_RESERVED_ZEROS[fields[0].spec_type]
        fields.pop(0)
    lines.append(f"{indent}    out = bytearray({bytes(prefix)!r})")
    position = 0
    while position < len(fields):
        field = fields[position]
        if field.spec_type == "bit":
            group = []
            while position < len(fields) and fields[position].spec_type == "bit":
                group.append(fields[position])
                position += 1
            live_bits = [(i, f) for i, f in enumerate(group) if not f.reserved]
            if not live_bits:
                lines.append(f'{indent}    out += b"\\x00"')
                continue
            lines.append(f"{indent}    bits = 0")
            for bit_position, bit_field in live_bits:
                lines.append(f"{indent}    if self.{bit_field.python_name}:")
                lines.append(f"{indent}        bits |= {1 << bit_position}")
            lines.append(f"{indent}    out.append(bits)")
            continue
        position += 1
        if field.reserved:
            if field.spec_type in _FIXED_RESERVED_ZEROS:
                zeros = b"\x00" * _FIXED_RESERVED_ZEROS[field.spec_type]
            elif field.spec_type == "longstr":
                zeros = b"\x00\x00\x00\x00"
            else:
                raise ValueError(f"reserved {field.spec_type} not supported")
            lines.append(f"{indent}    out += {zeros!r}")
            continue
        name = field.python_name
        match field.spec_type:
            case "octet":
                lines.append(f"{indent}    out.append(self.{name})")
            case "short":
                lines.append(f"{indent}    out += _PACK_SHORT(self.{name})")
            case "long":
                lines.append(f"{indent}    out += _PACK_LONG(self.{name})")
            case "longlong":
                lines.append(f"{indent}    out += _PACK_LONG_LONG(self.{name})")
            case "shortstr":
                lines.append(f"{indent}    _ENCODE_SHORTSTR(out, self.{name})")
            case "longstr":
                lines.append(f"{indent}    _ENCODE_LONGSTR(out, self.{name})")
            case "table":
                lines.append(f"{indent}    if self.{name}:")
                lines.append(f"{indent}        _ENCODE_TABLE(out, self.{name})")
                lines.append(f"{indent}    else:")
                lines.append(f'{indent}        out += b"\\x00\\x00\\x00\\x00"')
            case _:
                raise ValueError(f"unsupported method field type {field.spec_type}")
    lines.append(f"{indent}    return out")


def _emit_decoder(lines: list[str], method: Method) -> None:
    qualified = method.qualified_name
    class_part = method.class_name.replace("-", "_")
    method_part = method.spec_name.replace("-", "_")
    function_name = f"_decode_{class_part}_{method_part}"
    lines.append("")
    lines.append("")
    lines.append(f'def {function_name}(payload: bytes) -> "{qualified}":')
    live_fields = [f for f in method.fields if not f.reserved]
    if not live_fields:
        lines.append(f"    return _{function_name.upper().removeprefix('_DECODE_')}_INSTANCE")
        return
    lines.append(f"    method = {qualified}.__new__({qualified})")
    lines.append("    offset = 4")
    fields = list(method.fields)
    position = 0
    offset_known_end = False
    while position < len(fields):
        field = fields[position]
        if field.spec_type == "bit":
            group = []
            while position < len(fields) and fields[position].spec_type == "bit":
                group.append(fields[position])
                position += 1
            lines.append("    bits = payload[offset]")
            lines.append("    offset += 1")
            for bit_position, bit_field in enumerate(group):
                if bit_field.reserved:
                    continue
                bit_target = f"method.{bit_field.python_name}"
                lines.append(f"    {bit_target} = (bits & {1 << bit_position}) != 0")
            continue
        position += 1
        name = field.python_name
        if field.reserved:
            match field.spec_type:
                case "octet":
                    lines.append("    offset += 1")
                case "short":
                    lines.append("    offset += 2")
                case "long":
                    lines.append("    offset += 4")
                case "longlong":
                    lines.append("    offset += 8")
                case "shortstr":
                    lines.append("    offset += 1 + payload[offset]")
                case "longstr":
                    lines.append("    offset += 4 + _UNPACK_LONG(payload, offset)[0]")
                case _:
                    raise ValueError(f"reserved {field.spec_type} not supported")
            continue
        match field.spec_type:
            case "octet":
                lines.append(f"    method.{name} = payload[offset]")
                lines.append("    offset += 1")
            case "short":
                lines.append(f"    (method.{name},) = _UNPACK_SHORT(payload, offset)")
                lines.append("    offset += 2")
            case "long":
                lines.append(f"    (method.{name},) = _UNPACK_LONG(payload, offset)")
                lines.append("    offset += 4")
            case "longlong":
                lines.append(f"    (method.{name},) = _UNPACK_LONG_LONG(payload, offset)")
                lines.append("    offset += 8")
            case "shortstr":
                lines.append(f"    method.{name}, offset = _DECODE_SHORTSTR(payload, offset)")
            case "longstr":
                lines.append(f"    method.{name}, offset = _DECODE_LONGSTR(payload, offset)")
            case "table":
                lines.append(f"    method.{name}, offset = _DECODE_TABLE(payload, offset)")
            case _:
                raise ValueError(f"unsupported method field type {field.spec_type}")
        offset_known_end = position == len(fields)
    if offset_known_end:
        pass
    lines.append("    return method")


def _emit_method_class(lines: list[str], method: Method, response_indexes: tuple[int, ...]) -> None:
    indent = "    "
    live_fields = [f for f in method.fields if not f.reserved]
    lines.append("")
    lines.append(f"{indent}class {method.python_name}(Method):")
    lines.append(f'{indent}    """{method.class_name}.{method.spec_name}"""')
    lines.append("")
    slots = ", ".join(f'"{f.python_name}"' for f in live_fields)
    if len(live_fields) == 1:
        slots += ","
    lines.append(f"{indent}    __slots__ = ({slots})")
    lines.append("")
    lines.append(f"{indent}    index = {_method_index_comment(method.index)}")
    lines.append(f'{indent}    name = "{method.qualified_name}"')
    lines.append(f"{indent}    synchronous = {method.synchronous}")
    lines.append(f"{indent}    has_content = {method.has_content}")
    if response_indexes:
        formatted = ", ".join(f"0x{i:08X}" for i in response_indexes)
        trailer = "," if len(response_indexes) == 1 else ""
        lines.append(f"{indent}    valid_responses = ({formatted}{trailer})")
    if live_fields:
        lines.append("")
        lines.append(f"{indent}    def __init__(")
        lines.append(f"{indent}        self,")
        lines.append(f"{indent}        *,")
        for field in live_fields:
            python_type = _PYTHON_TYPE_BY_SPEC_TYPE[field.spec_type]
            lines.append(f"{indent}        {field.python_name}: {python_type},")
        lines.append(f"{indent}    ) -> None:")
        for field in live_fields:
            lines.append(f"{indent}        self.{field.python_name} = {field.python_name}")
    _emit_encode(lines, method)


def _emit_properties(lines: list[str], basic: SpecClass) -> None:
    live = [f for f in basic.properties if not f.reserved]
    flag_by_name = {}
    for position, field in enumerate(basic.properties):
        flag_by_name[field.python_name] = 1 << (15 - position)
    indent = "    "
    lines.append("")
    lines.append(f"{indent}class Properties:")
    lines.append(f'{indent}    """Basic content-header properties; unset fields stay ``None``."""')
    lines.append("")
    slots = ", ".join(f'"{f.python_name}"' for f in live)
    lines.append(f"{indent}    __slots__ = ({slots})")
    lines.append("")
    lines.append(f"{indent}    def __init__(")
    lines.append(f"{indent}        self,")
    lines.append(f"{indent}        *,")
    for field in live:
        base_type = _PYTHON_TYPE_BY_SPEC_TYPE[field.spec_type]
        if not base_type.endswith("| None"):
            base_type += " | None"
        lines.append(f"{indent}        {field.python_name}: {base_type} = None,")
    lines.append(f"{indent}    ) -> None:")
    for field in live:
        lines.append(f"{indent}        self.{field.python_name} = {field.python_name}")
    lines.append("")
    lines.append(f"{indent}    def __repr__(self) -> str:")
    lines.append(f"{indent}        fields = ', '.join(")
    lines.append(f"{indent}            f'{{name}}={{getattr(self, name)!r}}'")
    lines.append(f"{indent}            for name in self.__slots__")
    lines.append(f"{indent}            if getattr(self, name) is not None")
    lines.append(f"{indent}        )")
    lines.append(f"{indent}        return f'Properties({{fields}})'")
    lines.append("")
    lines.append(f"{indent}    def __eq__(self, other: object) -> bool:")
    lines.append(f"{indent}        if not isinstance(other, Basic.Properties):")
    lines.append(f"{indent}            return NotImplemented")
    lines.append(
        f"{indent}        return all("
        f"getattr(self, n) == getattr(other, n) for n in self.__slots__)"
    )
    lines.append("")
    lines.append(f"{indent}    def encode(self, out: bytearray) -> None:")
    lines.append(f'{indent}        """Append property flags + the set fields onto ``out``."""')
    lines.append(f"{indent}        flags = 0")
    lines.append(f"{indent}        flags_offset = len(out)")
    lines.append(f'{indent}        out += b"\\x00\\x00"')
    for field in live:
        flag = flag_by_name[field.python_name]
        name = field.python_name
        lines.append(f"{indent}        if self.{name} is not None:")
        lines.append(f"{indent}            flags |= 0x{flag:04X}")
        match field.spec_type:
            case "shortstr":
                lines.append(f"{indent}            _ENCODE_SHORTSTR(out, self.{name})")
            case "octet":
                lines.append(f"{indent}            out.append(self.{name})")
            case "table":
                lines.append(f"{indent}            _ENCODE_TABLE(out, self.{name})")
            case "timestamp":
                lines.append(f"{indent}            _ENCODE_TIMESTAMP(out, self.{name})")
            case _:
                raise ValueError(f"unsupported property type {field.spec_type}")
    lines.append(f"{indent}        _PACK_SHORT_INTO(out, flags_offset, flags)")
    lines.append("")
    lines.append(f"{indent}    @staticmethod")
    lines.append(
        f'{indent}    def decode(payload: bytes, offset: int) -> tuple["Basic.Properties", int]:'
    )
    lines.append(
        f'{indent}        """Read flags + announced fields; returns (properties, next offset)."""'
    )
    lines.append(f"{indent}        (flags,) = _UNPACK_SHORT(payload, offset)")
    lines.append(f"{indent}        offset += 2")
    lines.append(f"{indent}        # Bit 0 announces another (legal but unused) flags word.")
    lines.append(f"{indent}        more_flags = flags")
    lines.append(f"{indent}        while more_flags & 0x0001:")
    lines.append(f"{indent}            (more_flags,) = _UNPACK_SHORT(payload, offset)")
    lines.append(f"{indent}            offset += 2")
    lines.append(f"{indent}        properties = Basic.Properties.__new__(Basic.Properties)")
    for field in live:
        lines.append(f"{indent}        properties.{field.python_name} = None")
    for field in basic.properties:
        flag = flag_by_name[field.python_name]
        name = field.python_name
        lines.append(f"{indent}        if flags & 0x{flag:04X}:")
        if field.reserved:
            lines.append(f"{indent}            offset += 1 + payload[offset]")
            continue
        target = f"{indent}            properties.{name}"
        match field.spec_type:
            case "shortstr":
                lines.append(f"{target}, offset = _DECODE_SHORTSTR(payload, offset)")
            case "octet":
                lines.append(f"{target} = payload[offset]")
                lines.append(f"{indent}            offset += 1")
            case "table":
                lines.append(f"{target}, offset = _DECODE_TABLE(payload, offset)")
            case "timestamp":
                lines.append(f"{target}, offset = _DECODE_TIMESTAMP(payload, offset)")
            case _:
                raise ValueError(f"unsupported property type {field.spec_type}")
    lines.append(f"{indent}        return properties, offset")


def generate() -> str:
    """Build the complete spec.py source text."""
    constants, classes, domain_asserts = _parse(SPEC_XML)
    method_by_name: dict[str, Method] = {}
    for spec_class in classes:
        for method in spec_class.methods:
            method_by_name[f"{spec_class.spec_name}.{method.spec_name}"] = method

    lines: list[str] = []
    lines.append('"""AMQP 0-9-1 protocol definitions (RabbitMQ extended dialect).')
    lines.append("")
    lines.append("GENERATED FILE -- DO NOT EDIT BY HAND.")
    lines.append("Source: codegen/amqp0-9-1.extended.xml;")
    lines.append("regenerate with: python codegen/generate.py")
    lines.append('"""')
    lines.append("")
    lines.append("import datetime")
    lines.append("from collections.abc import Callable")
    lines.append("from typing import Any")
    lines.append("from typing import ClassVar")
    lines.append("from typing import TypedDict")
    lines.append("")
    lines.append("from amqpstorm_aio import encoding")
    lines.append("")
    lines.append("_PACK_SHORT = encoding.PACK_SHORT.pack")
    lines.append("_PACK_LONG = encoding.PACK_LONG.pack")
    lines.append("_PACK_LONG_LONG = encoding.PACK_LONG_LONG.pack")
    lines.append("_PACK_SHORT_INTO = encoding.PACK_SHORT.pack_into")
    lines.append("_UNPACK_SHORT = encoding.PACK_SHORT.unpack_from")
    lines.append("_UNPACK_LONG = encoding.PACK_LONG.unpack_from")
    lines.append("_UNPACK_LONG_LONG = encoding.PACK_LONG_LONG.unpack_from")
    lines.append("_ENCODE_SHORTSTR = encoding.encode_shortstr")
    lines.append("_ENCODE_LONGSTR = encoding.encode_longstr")
    lines.append("_ENCODE_TABLE = encoding.encode_table")
    lines.append("_ENCODE_TIMESTAMP = encoding.encode_timestamp")
    lines.append("_DECODE_SHORTSTR = encoding.decode_shortstr")
    lines.append("_DECODE_LONGSTR = encoding.decode_longstr")
    lines.append("_DECODE_TABLE = encoding.decode_table")
    lines.append("_DECODE_TIMESTAMP = encoding.decode_timestamp")
    lines.append("")

    for name, value, _classification in constants:
        lines.append(f"{name.replace('-', '_').upper()} = {value}")
    soft = sorted(value for _n, value, c in constants if c == "soft-error")
    hard = sorted(value for _n, value, c in constants if c == "hard-error")
    lines.append("")
    lines.append("# Soft errors close the channel; hard errors close the whole connection.")
    lines.append(f"SOFT_ERROR_CODES = frozenset({soft!r})")
    lines.append(f"HARD_ERROR_CODES = frozenset({hard!r})")
    _emit_asserts(lines, classes, domain_asserts)
    _emit_properties_dict(lines, classes)
    lines.append("")
    lines.append("")
    lines.append("class Method:")
    lines.append('    """Base of every generated method class."""')
    lines.append("")
    lines.append("    __slots__: ClassVar[tuple[str, ...]] = ()")
    lines.append("")
    lines.append("    index: ClassVar[int] = 0")
    lines.append('    name: ClassVar[str] = ""')
    lines.append("    synchronous: ClassVar[bool] = False")
    lines.append("    has_content: ClassVar[bool] = False")
    lines.append("    valid_responses: ClassVar[tuple[int, ...]] = ()")
    lines.append("")
    lines.append("    def encode(self) -> bytearray:")
    lines.append('        """Return the method-frame payload (class + method + arguments)."""')
    lines.append("        raise NotImplementedError")
    lines.append("")
    lines.append("    def __repr__(self) -> str:")
    lines.append("        fields = ', '.join(f'{n}={getattr(self, n)!r}' for n in self.__slots__)")
    lines.append("        return f'{self.name}({fields})'")
    lines.append("")
    lines.append("    def __eq__(self, other: object) -> bool:")
    lines.append("        if type(other) is not type(self):")
    lines.append("            return NotImplemented")
    equality = "all(getattr(self, n) == getattr(other, n) for n in self.__slots__)"
    lines.append(f"        return {equality}")

    for spec_class in classes:
        lines.append("")
        lines.append("")
        lines.append(f"class {spec_class.python_name}:")
        lines.append(
            f'    """Methods of the {spec_class.spec_name} class (index {spec_class.index})."""'
        )
        if spec_class.spec_name == "basic":
            _emit_properties(lines, spec_class)
        for method in spec_class.methods:
            response_indexes = tuple(
                method_by_name[f"{spec_class.spec_name}.{response}"].index
                for response in method.response_names
            )
            _emit_method_class(lines, method, response_indexes)

    # No-field methods decode to shared singletons: nothing distinguishes instances.
    lines.append("")
    lines.append("")
    for spec_class in classes:
        for method in spec_class.methods:
            if not any(not f.reserved for f in method.fields):
                raw_name = f"_{spec_class.spec_name}_{method.spec_name}_INSTANCE"
                constant = raw_name.replace("-", "_").upper()
                lines.append(f"{constant} = {method.qualified_name}()")

    for spec_class in classes:
        for method in spec_class.methods:
            _emit_decoder(lines, method)

    lines.append("")
    lines.append("")
    lines.append("METHOD_DECODERS: dict[int, Callable[[bytes], Method]] = {")
    for spec_class in classes:
        for method in spec_class.methods:
            function_name = (
                f"_decode_{spec_class.spec_name}_{method.spec_name}".replace("-", "_")
            )
            lines.append(f"    0x{method.index:08X}: {function_name},")
    lines.append("}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    source = generate()
    OUTPUT_PATH.write_text(source, encoding="utf-8", newline="\n")
    print(f"wrote {OUTPUT_PATH} ({len(source.splitlines())} lines)")


if __name__ == "__main__":
    main()
