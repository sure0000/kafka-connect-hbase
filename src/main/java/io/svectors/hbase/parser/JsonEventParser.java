/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.svectors.hbase.parser;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.base.Preconditions;

/**
 * Parses a json event.
 * 
 */
public class JsonEventParser implements EventParser {

	private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private final static ObjectReader JSON_READER = OBJECT_MAPPER.reader(JsonNode.class);
	private JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
	private final JsonConverter keyConverter;
	private final JsonConverter valueConverter;
	static Logger logger = Logger.getLogger(JsonEventParser.class);

	/**
	 * default c.tor
	 */
	public JsonEventParser() {
		OBJECT_MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
		this.keyConverter = new JsonConverter();
		this.valueConverter = new JsonConverter();

		Map<String, String> props = new HashMap<>(1);
		props.put("schemas.enable", Boolean.FALSE.toString());

		this.keyConverter.configure(props, true);
		this.valueConverter.configure(props, false);

	}

	@Override
	public Map<String, byte[]> parseKey(SinkRecord sr) throws EventParsingException {
		// return this.parse(sr.topic(), sr.keySchema(), sr.key(), true);
		return this.parse(sr.topic(), null, sr.key(), true);

	}

	@Override
	public Map<String, byte[]> parseValue(SinkRecord sr) throws EventParsingException {
		return this.parse(sr.topic(), sr.valueSchema(), sr.value(), false);
	}

	/**
	 * Parses the value.
	 * 
	 * @param topic
	 * @param schema
	 * @param value
	 * @return
	 * @throws EventParsingException
	 */
	public Map<String, byte[]> parse(final String topic, final Schema schema, final Object value, final boolean isKey)
			throws EventParsingException {
//		Map<String,String> stringMapValue = new HashMap<>();
//		if (value != null) {
//			((HashMap<String,Object>) value).forEach((k,v) -> {
//				if (v instanceof Integer) {
//					stringMapValue.put(k, ((Integer)v).toString());
//				}else if (v instanceof Long) {
//					stringMapValue.put(k, ((Long)v).toString());
//				}else{
//					stringMapValue.put(k,v.toString());
//				}
//			});
//		}
		final Map<String, byte[]> values = new LinkedHashMap<>();
		try {
			byte[] valueBytes = null;
			if (isKey) {
				if (schema != null) {
					valueBytes = keyConverter.fromConnectData(topic, schema, value);
				} else {
					valueBytes = keyConverter.fromConnectData(topic, null, value);
				}
			} else {
				if (schema != null) {
					valueBytes = valueConverter.fromConnectData(topic, schema, value);
				} else {
					valueBytes = valueConverter.fromConnectData(topic, null, value);
				}

			}
			JsonNode valueNode = jsonNodeFactory.objectNode();

			Map<String, Object> keyValues = new HashMap<>();
			if (valueBytes == null || valueBytes.length == 0) {
				keyValues = OBJECT_MAPPER.convertValue(valueNode, new TypeReference<Map<String, Object>>() {
				});

			} else {
				valueNode = JSON_READER.readValue(valueBytes);
			}
			if (schema != null) {
				keyValues = OBJECT_MAPPER.convertValue(valueNode, new TypeReference<Map<String, Object>>() {
				});
				final List<Field> fields = schema.fields();
				for (Field field : fields) {
					final byte[] fieldValue = toValue(keyValues, field);
					if (fieldValue == null) {
						continue;
					}
					values.put(field.name(), fieldValue);
				}
			} else {
				 
				if (!valueNode.isTextual()) {
					keyValues = OBJECT_MAPPER.convertValue(valueNode, new TypeReference<Map<String, Object>>() {

					});
				}
				keyValues.entrySet().forEach(entry -> {
					values.put(entry.getKey(), toValue(entry.getValue()));
				});
			}
			return values;
		} catch (Exception ex) {
			ex.printStackTrace();
			final String errorMsg = String.format("Failed to parse the schema [%s] , value [%s] with ex [%s]", schema,
					value, ex.getMessage());
			throw new EventParsingException(errorMsg, ex);
		}
	}

	/**
	 * Convert value to byte values based on the Schema field type
	 * 
	 * @param keyValues
	 * @param field
	 * @return
	 */
	private byte[] toValue(final Map<String, Object> keyValues, final Field field) {
		Preconditions.checkNotNull(field);
		final Schema.Type type = field.schema().type();
		final String fieldName = field.name();
		final Object fieldValue = keyValues.get(fieldName);
		//  20201229 changed by xuyouchang: 默认以字符类型插入hbase
//		switch (type) {
//		case STRING:
//			return Bytes.toBytes((String) fieldValue);
//		case BOOLEAN:
//			return Bytes.toBytes((Boolean) fieldValue);
//		case BYTES:
//			return Bytes.toBytes((ByteBuffer) fieldValue);
//		case FLOAT32:
//			return Bytes.toBytes((Float) fieldValue);
//		case FLOAT64:
//			return Bytes.toBytes((Double) fieldValue);
//		case INT8:
//			return Bytes.toBytes((Byte) fieldValue);
//		case INT16:
//			return Bytes.toBytes((Short) fieldValue);
//		case INT32:
//			return Bytes.toBytes((Integer) fieldValue);
//		case INT64:
//			return Bytes.toBytes((Long) fieldValue);
//		// case MAP:
//		// return Bytes.toBytes(new JSONObject((Map)fieldValue).toString());
//		default:
//			return null;
//		}
		return Bytes.toBytes((String) fieldValue);
	}

	/**
	 * Get value based on the instance type and convert to bytes
	 * 
	 * @param fieldValue
	 * @return
	 */
	private byte[] toValue(Object fieldValue) {
		Preconditions.checkNotNull(fieldValue);
		// 20201229 changed by xuyouchang: 默认以字符类型插入hbase
//		Class<?> clazz = fieldValue.getClass();
//		String type = clazz.getSimpleName().toUpperCase();
//		switch (type) {
//		case "STRING":
//			return Bytes.toBytes((String) fieldValue);
//		case "BOOLEAN":
//			return Bytes.toBytes((Boolean) fieldValue);
//		case "BYTES":
//			return Bytes.toBytes((ByteBuffer) fieldValue);
//		case "FLOAT":
//			return Bytes.toBytes((Float) fieldValue);
//		case "SHORT":
//			return Bytes.toBytes((Short) fieldValue);
//		case "INTEGER":
//			return Bytes.toBytes((Integer) fieldValue);
//		case "LONG":
//			return Bytes.toBytes((Long) fieldValue);
//		// case MAP:
//		// return Bytes.toBytes(new JSONObject((Map)fieldValue).toString());
//		default:
//			return null;
//		}
		return Bytes.toBytes((String) fieldValue);
	}
}
