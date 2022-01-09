from typing import List, Dict, Any, Tuple, Optional, Generic, TypeVar, Callable, Protocol, Type

from dataclasses import dataclass

import boto3
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from boto3.dynamodb.conditions import Key, AttributeBase
from pydantic import BaseModel

_serializer = TypeSerializer()
_deserializer = TypeDeserializer()


def serialize(obj: Dict[str, Any]):
    return _serializer.serialize(obj)["M"]


def deserialize(obj: Dict[str, Any]):
    return _deserializer.deserialize({"M": obj})


@dataclass()
class KeygenRules:
    pk: List[str]
    sk: List[str]


def generate_keys_with_rules(rules: List[Any], d: Dict[str, Any]):
    resp = {}
    for rulek, rulefields in rules:
        parts = []

        for field in rulefields:
            if field.startswith("!"):
                parts.append(field[1:])
                continue

            val = d[field]
            # first None stops key generation. use for begins_with etc
            if val is None:
                break
            parts.append(field)
            parts.append(str(val))

        resp[rulek] = "#".join(parts)
    return resp


def ddb_table(tablename: str) -> Any:
    return boto3.resource("dynamodb").Table(tablename)


@dataclass()
class TableSpec:
    name: str
    pk: str = "PK"
    sk: str = "SK"
    type_col: Optional[str] = None


# first rule is always pk rule, then sk rule
def parse_rules(db: TableSpec, rules: KeygenRules):
    return [(db.pk, rules.pk), (db.sk, rules.sk)]


# these are actually classes, not instances
class ModelProtocol(Protocol):
    def dict(self) -> dict: ...

class QueryModel(BaseModel):
    ...


TKey = TypeVar("TKey", bound=BaseModel)
TVal = TypeVar("TVal", bound=BaseModel)


class Dao(Generic[TKey, TVal]):
    def __init__(self, valclass: Type[TVal], spec: TableSpec):
        self.valclass = valclass

        rules: KeygenRules = valclass.get_rules() # type: ignore
        self.rules = parse_rules(spec, rules)
        self.spec = spec

    def _table(self):
        return ddb_table(self.spec.name)

    def add(self, obj: TVal):
        d = obj.dict()
        keys = generate_keys_with_rules(self.rules, d)
        d.update(keys)
        if self.spec.type_col:
            d[self.spec.type_col] = type(obj).__name__
        self._table().put_item(Item=d, ConditionExpression=f"attribute_not_exists({self.spec.pk})")

    def get(self, key: TKey) -> Optional[TVal]:
        keyd = key.dict()
        key = generate_keys_with_rules(self.rules, keyd)
        got = self._table().get_item(Key=key).get("Item")
        if got is None:
            return None
        return self.valclass(**got)

    def update(self, key: TKey, update_dict: Dict[str, Any]):
        keyd = key.dict()
        key = generate_keys_with_rules(self.rules, keyd)


        expr = []
        attrnames = {}
        vals = {}
        for i, (k,v) in enumerate(update_dict.items()):
            expr.append(f"#attr{i} = :val{i}")
            attrnames[f"#attr{i}"] = k
            vals[f":val{i}"] = v




        res = self._table().update_item(
                                    Key=key,
                                    UpdateExpression="set " + ", ".join(expr),
                                    ExpressionAttributeNames=attrnames,
                                    ExpressionAttributeValues=vals,
                                    ConditionExpression=f"attribute_exists({self.spec.pk})"


        )

        return res

    def query_pk(self, key: TKey):
        # this will stop at first None
        pkval = generate_keys_with_rules([self.rules[0]], key.dict())[self.spec.pk]
        got = self._table().query(
            KeyConditionExpression=Key(self.spec.pk).eq(pkval))

        return got

    def query_beg(self, key: TKey):
        return self.query_cond(key, lambda k, val: k.begins_with(val))

    def query_cond(self, key: TKey, cond_function: Callable[[Key, str], Any]):

        # can write more complex queries using both pk and sk
        keyd = key.dict()
        pkval = generate_keys_with_rules([self.rules[0]], keyd)[self.spec.pk]
        skval = generate_keys_with_rules([self.rules[1]], keyd)[self.spec.sk]

        other_cond = cond_function(Key(self.spec.sk), skval)

        res = self._table().query(
            KeyConditionExpression=Key(self.spec.pk).eq(pkval) & other_cond)
        return res.get("Items")
