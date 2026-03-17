from peewee import SqliteDatabase, Model, PrimaryKeyField, CharField, TextField, IntegerField, ForeignKeyField, \
    DateTimeField, EXCLUDED, Case, Expression, OP, DecimalField
from psycopg.sql import SQL, Identifier, Placeholder, Literal
from datetime import datetime, timezone, timedelta, UTC
from code.settings import DATA_PATH
from code.connections import db_source, db_destination
import json



dest_db = SqliteDatabase(DATA_PATH / 'destinations' / 'db_destination.db', pragmas={'foreign_key': 'ON'})

DATETIME_FORMAT ='%Y-%m-%d %H:%M:%S.%f'

# Help Text
CUMULATIVE = "CUMULATIVE"
PROGRESS = "PROGRESS"
SKIP_IF_NULL = "SKIP_IF_NULL"
CONCAT = "CONCAT"

# Foreign key on delete
SET_NULL = "SET NULL"
CASCADE = "CASCADE"


class JsonField(TextField):
    field_type = 'JSON'
    
    def adapt(self, value):
        if type(value) == dict:
            val = json.dumps(value)
            return val
        else:
            val = super().adapt(value)
            try:
                json.loads(val)
                return val
            except Exception as err:
                raise ValueError(f'Value is not JSON serializable: {value}')
    
    def python_value(self, value):
        if not value:
            return None
        val = json.loads(super().python_value(value))
        return val


class AutoDateTimeField(DateTimeField):
    pass

class BaseModel(Model):
    created_at = DateTimeField(formats=['%Y-%m-%d %H:%M:%S'], null=False)
    updated_at = DateTimeField(formats=['%Y-%m-%d %H:%M:%S'], null=True)
    deleted_at = DateTimeField(formats=['%Y-%m-%d %H:%M:%S'], null=True)
    synced_at = AutoDateTimeField(formats=['%Y-%m-%d %H:%M:%S'], null=False, default=datetime.now)

    class Meta:
        legacy_table_names = False
    
    @classmethod
    def create_all_tables(cls):
        for Mdl in cls.__subclasses__(): Mdl.create_table(safe=True)
        
    @classmethod
    def update(cls, data=None, **update):
        if data and cls.synced_at.column_name in data:
            return super().update(data, **update)
        if cls.synced_at.column_name in update:
            return super().update(data, **update)
        return super().update(data, **update)
    

    @classmethod
    def get_update_expression(cls, insert):
        cols = cls._meta.columns
        pks = cls._meta.get_primary_keys()
        update = {}
        for c, v in cols.items():
            # Only update the inserted columns, to avoid replacement by default values
            if c in insert or c == cls.synced_at.column_name:
                if v.help_text == CUMULATIVE:
                    update[c] = EXCLUDED.__getattr__(c).__add__(v)
                # Do not replace non-null with null
                elif v.help_text == PROGRESS:
                    update[c] = Case(predicate=None, expression_tuples=[(Expression(v, OP.IS, None), EXCLUDED.__getattr__(c))], default=v )
                elif v.help_text == SKIP_IF_NULL:
                    update[c] = Case(predicate=None, expression_tuples=[(Expression(EXCLUDED.__getattr__(c), OP.IS_NOT, None), EXCLUDED.__getattr__(c))], default=v )
                elif v.help_text == CONCAT:
                    update[c] = Case(predicate=None, 
                                     expression_tuples=[(Expression(v, OP.IS, None), EXCLUDED.__getattr__(c)),
                                                        (Expression(EXCLUDED.__getattr__(c), OP.IS_NOT, None),  Expression(v, OP.CONCAT, Expression(', ', OP.CONCAT, EXCLUDED.__getattr__(c))) ) ], 
                                     default=v )
                else:
                    update[c] = EXCLUDED.__getattr__(c)
        return pks, update

    @classmethod
    def upsert(cls, __data=None, **insert):
        pks, update = cls.get_update_expression(__data or insert)
        if not pks:
            return cls.insert(__data, **insert)
        return cls.insert(__data, **insert).on_conflict(conflict_target=pks, update=update)
    
    @classmethod
    def upsert_many(cls, rows, fields=None):
        pks, update = cls.get_update_expression(rows[0])
        if not pks:
            return cls.insert_many(rows=rows, fields=fields)
        return cls.insert_many(rows=rows, fields=fields).on_conflict(conflict_target=pks, update=update)
    
    @classmethod
    def update_many(cls, data: list[dict]):
        cls._is_updated = True
        if not data:
            return
        
        pks = cls._meta.get_primary_keys()
        # Do not perform update_many without a primary key! Too slow!
        if not pks:
            raise ValueError('update_many failed: model %s does not have a primary key' %cls.__name__)
        
        pk_name = pks[0].column_name
        cols = cls._meta.columns

        sample = data[0]
        cols_list = []

        for k in sample.keys():
            if cols[k].help_text == CUMULATIVE:
                cols_update_sql = SQL(' {} = {} + {} ').format(Identifier(k), Identifier(k), Placeholder(k))
            elif cols[k].help_text == PROGRESS:
                cols_update_sql = SQL(' {} = case when {} is null then {} else {} end ').format(Identifier(k), Identifier(k), Placeholder(k), Identifier(k))
            elif cols[k].help_text == SKIP_IF_NULL:
                cols_update_sql = SQL(' {} = case when {} is null then {} else {} end ').format(Identifier(k), Placeholder(k), Identifier(k), Placeholder(k))
            else:
                cols_update_sql = SQL(' {} = {} ').format(Identifier(k), Placeholder(k))
            cols_list.append(cols_update_sql)

        sync_col = cls.synced_at.column_name
        if sync_col not in sample:
            now_utc = datetime.now(UTC)
            cols_list.append(SQL(' {} = {} ').format(Identifier(sync_col), Literal(now_utc) ))

        q = SQL("update {} set {} where {} = {}")\
            .format(
                Identifier(cls._meta.schema, cls._meta.table_name),
                SQL(', ').join(cols_list),
                Identifier(pk_name),
                Placeholder(pk_name)
            )
        db_destination.executemany(q, parameters=data)
        return True


class CarSalesDataset(BaseModel):
    sale_id = IntegerField(primary_key=True, null=False)
    brand_name = CharField(max_length=200, null=False)
    car_name = CharField(max_length=200, null=False)
    sale_price = DecimalField(max_digits=12, decimal_places=2, auto_round=True, null=False)
    sale_price_currency = CharField(max_length=3, null=False)
    sale_price_normalized = DecimalField(max_digits=12, decimal_places=2, auto_round=True, null=False)
    sale_price_normalized_currency = CharField(max_length=3, null=False)
    exchange_rate_at = DateTimeField(null=False)
    transaction_date = DateTimeField(null=False)


for Mdl in BaseModel.__subclasses__():
    if not Mdl.table_exists():
        Mdl.create_table(safe=True)