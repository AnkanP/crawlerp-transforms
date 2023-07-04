# crawlerp-transforms



 	 "transforms": "RawToData,InsertField",
"transforms.InsertField.type": "org.apache.kafka.connect.transforms.InsertField$Value",
"transforms.InsertField.static.field": "MessageSource",
"transforms.InsertField.static.value": "Kafka Connect framework",
"transforms.InsertField.predicate": "IsTombstone",
"transforms.InsertField.negate": "true",


"predicates": "IsTombstone",
"predicates.IsTombstone.type": "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone",

"transforms.RawToData.type": "kafka.connect.transforms.kafka.Raw2DataTransformer$Value",
"transforms.RawToData.predicate": "IsTombstone",
"transforms.RawToData.negate": "true",

"transforms.RawToData.op.field.name": "operation",
"transforms.RawToData.raw2data.timestamp.field.name" : "tc"
