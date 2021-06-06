from typing import Type, Container, Optional

import pandas as pd
from pandas.core.dtypes.cast import convert_dtypes
from pydantic import BaseModel, create_model, BaseConfig
from sqlalchemy import inspect
from sqlalchemy.orm import ColumnProperty


class OrmConfig(BaseConfig):
    orm_mode = True


class SchemaBase(BaseModel):
    base: Type[BaseModel]
    create: Type[BaseModel]
    edit: Type[BaseModel]
    instance: Type[BaseModel]
    id_type: Type = str

    @classmethod
    def simple(cls, schema: Type[BaseModel]):
        return cls(base=schema, create=schema, edit=schema, instance=schema)

    def converter(self, entry, schema_type=None):
        if schema_type is None:
            schema_type = self.instance
        return schema_type(**entry)


class PandasSchema(SchemaBase):
    def converter(self, entry, schema_type=None):
        if schema_type is None:
            schema_type = self.instance
        return [schema_type(**v.dropna().to_dict()) for k, v in entry.iterrows()]


def orm2pydantic(db_model: Type, *,
                 config: Type = OrmConfig,
                 exclude: Container[str] = []) -> SchemaBase:
    mapper = inspect(db_model)
    fields = {}
    for attr in mapper.attrs:
        if isinstance(attr, ColumnProperty):
            if attr.columns:
                name = attr.key
                if name in exclude:
                    continue
                column = attr.columns[0]
                python_type: Optional[type] = None
                if hasattr(column.type, "impl"):
                    if hasattr(column.type.impl, "python_type"):
                        python_type = Optional[column.type.impl.python_type]
                elif hasattr(column.type, "python_type"):
                    python_type = Optional[column.type.python_type]
                assert python_type, f"Could not infer python_type for {column}"
                default = None
                # if column.default is None and not column.nullable:
                #     default = ...
                fields[name] = (python_type, default)
    # noinspection PyTypeChecker
    pydantic_model = create_model(db_model.__name__, __config__=config, **fields)
    return SchemaBase(
        base=pydantic_model,
        create=pydantic_model,
        edit=pydantic_model,
        instance=pydantic_model)


_pd_conversion_map = {
    'string': str,
    'boolean': bool,
    'integer': int,
    'float': float,
    'numeric': float,
    'Int64': int,
    'Float64': float,
}


def pd2pydantic(model_name: str, df: pd.DataFrame,
                config: Type = OrmConfig,
                exclude: Container[str] = [],
                column_id: str = 'id',
                ) -> PandasSchema:
    fields = {}
    ndf = df.reset_index()
    for name in ndf.columns:
        python_type = _pd_conversion_map[str(convert_dtypes(ndf[name]))]
        if name in exclude:
            continue
        python_type = Optional[python_type]
        default = None
        # if column.default is None and not column.nullable:
        #     default = ...
        fields[name] = (python_type, default)
    # noinspection PyTypeChecker
    pydantic_model = create_model(model_name, **fields)
    fields.pop(column_id, None)
    pydantic_model_no_id = create_model(model_name + '_no_id', **fields)
    return PandasSchema(
        base=pydantic_model,
        create=pydantic_model,
        edit=pydantic_model_no_id,
        instance=pydantic_model)
