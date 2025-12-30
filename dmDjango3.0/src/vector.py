from django.core import checks
from django import forms
from django.db.backends.utils import truncate_name
from django.db.models.expressions import RawSQL
from django.db.backends.ddl_references import Statement, Columns, Table, IndexName
from django.db.models import Field, FloatField, Func, Index

MAX_DIM_LENGTH = 65535
MIN_DIM_LENGTH = 1

def encode_vector(value, dim=None):
    import numpy
    if value is None:
        return value

    if dim is not None and len(value) != dim:
        raise ValueError(f"expected {dim} dimensions, but got {len(value)}")

    if isinstance(value, numpy.ndarray):
        if value.ndim != 1:
            raise ValueError("expected ndim to be 1")
        return f"[{','.join(map(str, value))}]"

    return str(value)

def decode_vector(value: str):
    import numpy
    if value is None:
        return value

    if value == "[]":
        return numpy.array([], dtype=numpy.float32)

    return numpy.array(value[1:-1].split(","), dtype=numpy.float32)

class VectorField(Field):
    description = "Vector"
    empty_strings_allowed = False

    def __init__(self, *args, dim: int = None, format: str = None, **kwargs):
        self.dim = dim
        self.format = format
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if self.dim is not None:
            kwargs["dim"] = self.dim
        return name, path, args, kwargs

    def db_type(self, connection):
        if self.dim is None:
            return "VECTOR"
        elif self.format is None:
            return f"VECTOR({self.dim}, FLOAT32)"
        else:
            return f"VECTOR({self.dim}, {self.format})"

    def from_db_value(self, value, expression, connection):
        return decode_vector(value)

    def to_python(self, value):
        import numpy
        if isinstance(value, list):
            return numpy.array(value, dtype=numpy.float32)
        return decode_vector(value)

    def get_prep_value(self, value):
        return encode_vector(value)

    def value_to_string(self, obj):
        return self.get_prep_value(self.value_from_object(obj))

    def validate(self, value, model_instance):
        import numpy
        if isinstance(value, numpy.ndarray):
            value = value.tolist()
        super().validate(value, model_instance)

    def run_validators(self, value):
        import numpy
        if isinstance(value, numpy.ndarray):
            value = value.tolist()
        super().run_validators(value)

    def formfield(self, **kwargs):
        return super().formfield(form_class=VectorFormField, **kwargs)

    def check(self, **kwargs):
        return [
            *super().check(**kwargs),
            *self._check_dimensions(),
            *self._check_format(),
        ]

    def _check_dimensions(self):
        if self.dim is None or not isinstance(self.dim, int):
            return [
                checks.Error(
                    f"Dimension must be of type integer and cannot be None",
                    obj=self,
                )
            ]

        if self.dim < MIN_DIM_LENGTH or self.dim > MAX_DIM_LENGTH:
            return [
                checks.Error(
                    f"Vector dimensions must be in the range [{MIN_DIM_LENGTH}, {MAX_DIM_LENGTH}]",
                    obj=self,
                )
            ]
        return []

    def _check_format(self):
        if self.format is None:
            self.format = 'FLOAT32'

        if not isinstance(self.format, str):
            return [
                checks.Error(
                    f"Format must be a string type or None",
                    obj=self,
                )
            ]

        if self.format not in ['INT8', 'FLOAT32', 'FLOAT64']:
            return [checks.Error(
                f"Vector format must be in ['INT8', 'FLOAT32', 'FLOAT64']"
            )]

        return []

class IvfVectorIndex(Index):
    def __init__(
        self,
        *expressions,
        fields = (),
        name: str = None,
        metric_name: str = "COSINE",
        percentage_value: int = 90,
        num_of_partitions: int = None,
        db_tablespace = None,
        opclasses = (),
        condition = None,
        **kwargs
    ) -> None:
        if fields == ():
            raise ValueError(
                "The index column must be specified"
            )
        if isinstance(fields, str):
            self.fields = [fields]
        elif isinstance(fields, (list, tuple)):
            if len(fields) != 1 or type(fields[0]) is not str:
                raise ValueError(
                "The index column is only allowed to be one column and must be declared as string"
            )
            else:
                self.fields = fields
        else:
            raise ValueError("Index.fields must be a list, a tuple or a string")
        if name is None:
            self.name = "ivf_ind" + str(self.fields[0])
        else:
            if not isinstance(name, str):
                raise ValueError("Index name must be a string")
            else:
                self.name = name
        self._metric_name = metric_name
        self._percentage_value = percentage_value
        self._num_of_partitions = num_of_partitions

        super().__init__(*expressions, fields=self.fields, name=self.name, db_tablespace=db_tablespace, opclasses = opclasses,
        condition = condition, **kwargs)

    def quote_name(self, name):
        if not name.startswith('"') or not name.endswith('"'):
            name = name.replace('"', '""')
            name = '"%s"' % truncate_name(name.upper(), 128)

        return name.upper()

    def create_sql(self, model, schema_editor, using="", **kwargs):
        table = model._meta.db_table
        fields = [
            model._meta.get_field(field_name)
            for field_name, _ in self.fields_orders
        ]
        columns = [field.column for field in fields]

        sql_template = "CREATE VECTOR INDEX %(name)s on %(table)s(%(columns)s) ORGANIZATION PARTITIONS\n"\
            "DISTANCE %(metric_name)s WITH TARGET ACCURACY %(percentage_value)s"

        if self._num_of_partitions is not None:
            sql_template += "PARAMETERS(TYPE IVF, NEIGHBOR PARTITIONS " + str(self._num_of_partitions) + ");"

        columns=(Columns(table, columns, self.quote_name, col_suffixes=()))

        def create_index_name(*args, **kwargs):
            return self.quote_name(self.name)

        return Statement(
            sql_template,
            fields=fields,
            table=Table(table, self.quote_name),
            name=IndexName(table, columns, self.suffix, create_index_name),
            columns=columns,
            metric_name = self._metric_name,
            percentage_value = self._percentage_value,
            **kwargs,
        )

class HnswVectorIndex(Index):

    def __init__(
        self,
        *expressions,
        fields = (),
        name: str = None,
        skip_existing: bool = False,
        metric_name: str = "COSINE",
        percentage_value: int = 90,
        max_connection: int = None,
        ef_construction: int = None,
        db_tablespace = None,
        opclasses = (),
        condition = None,
        include = None,
    ) -> None:
        if fields == ():
            raise ValueError(
                "The index column must be specified"
            )
        if isinstance(fields, str):
            self.fields = [fields]
        elif isinstance(fields, (list, tuple)):
            if len(fields) != 1 or type(fields[0]) is not str:
                raise ValueError(
                "The index column is only allowed to be one column and must be declared as string"
            )
            else:
                self.fields = fields
        else:
            raise ValueError("Index.fields must be a list, a tuple or a string")
        if name is None:
            self.name = "hnsw_ind" + str(self.fields[0])
        else:
            if not isinstance(name, str):
                raise ValueError("Index name must be a string")
            else:
                self.name = name
        self._skip_existing = skip_existing
        self._metric_name = metric_name
        self._percentage_value = percentage_value
        self._max_connection = max_connection
        self._ef_construction = ef_construction

        super().__init__(*expressions, fields=self.fields, name=self.name, db_tablespace=db_tablespace, opclasses = opclasses,
        condition = condition, include = include)

    def quote_name(self, name):
        if not name.startswith('"') or not name.endswith('"'):
            name = name.replace('"', '""')
            name = '"%s"' % truncate_name(name.upper(), 128)

        return name.upper()

    def create_sql(self, model, schema_editor, using="", **kwargs):
        table = model._meta.db_table
        fields = [
            model._meta.get_field(field_name)
            for field_name, _ in self.fields_orders
        ]
        columns = [field.column for field in fields]

        sql_template = "CREATE VECTOR INDEX %(name)s on %(table)s(%(columns)s) ORGANIZATION GRAPH\n"\
            "DISTANCE %(metric_name)s WITH TARGET ACCURACY %(percentage_value)s"

        if self._max_connection is not None or self._ef_construction is not None:
            if self._max_connection is not None:
                sql_template += "PARAMETERS(TYPE HNSW, NEIGHBOR " + str(self._max_connection)
                if self._ef_construction is not None:
                    sql_template += ", EFCONSTRUCTION " + str(self._ef_construction) + ");"
                else:
                    sql_template += ");"
            else:
                sql_template += "PARAMETERS(TYPE HNSW, EFCONSTRUCTION " + str(self._ef_construction) + ");"

        columns=(Columns(table, columns, self.quote_name, col_suffixes=()))

        def create_index_name(*args, **kwargs):
            return self.quote_name(self.name)

        return Statement(
            sql_template,
            fields=fields,
            table=Table(table, self.quote_name),
            name=IndexName(table, columns, self.suffix, create_index_name),
            columns=columns,
            metric_name = self._metric_name,
            percentage_value = self._percentage_value,
            **kwargs,
        )

class distance_func(Func):
    output_field = FloatField()

    def __init__(self, expression, vector=None, **extra):

        if not hasattr(expression, "field") or not isinstance(expression.field, VectorField):
            raise ValueError(
                "Expect Vector Column"
            )
        expressions = [expression.field.column]
        if vector is not None:
            formatted_other = encode_vector(vector)
            with_sign_str = "TO_VECTOR(\'" + formatted_other + "\', " + str(
                expression.field.dim) + ", " + expression.field.format + ")"
            vector = RawSQL(with_sign_str, [])
            expressions.append(vector)
        super().__init__(*expressions, **extra)

class l1_distance(distance_func):
    function = "L1_DISTANCE"

class l2_distance(distance_func):
    function = "L2_DISTANCE"

class cosine_distance(distance_func):
    function = "COSINE_DISTANCE"

class hamming_distance(distance_func):
    function = "HAMMING_DISTANCE"

class inner_product(distance_func):
    function = "INNER_PRODUCT"

class inner_product_negative(distance_func):
    function = "INNER_PRODUCT_NEGATIVE"

class VectorWidget(forms.TextInput):
    def format_value(self, value):
        import numpy
        if isinstance(value, numpy.ndarray):
            value = value.tolist()
        return super().format_value(value)

class VectorFormField(forms.CharField):
    widget = VectorWidget

    def has_changed(self, initial, data):
        import numpy
        if isinstance(initial, numpy.ndarray):
            initial = initial.tolist()
        return super().has_changed(initial, data)
