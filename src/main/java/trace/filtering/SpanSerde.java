/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package trace.filtering;

import com.google.protobuf.InvalidProtocolBufferException;
import io.jaegertracing.api_v2.Model.Span;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class SpanSerde implements Serde<Span> {

	private static final Serializer<Span> SERIALIZER = new Serializer<Span>() {

		@Override
		public void configure(Map<String, ?> map, boolean bln) {
		}

		@Override
		public byte[] serialize(String string, Span span) {
			if (span != null) {
				return span.toByteArray();
			}
			return null;
		}

		@Override
		public void close() {
		}
	};

	private static final Deserializer<Span> DESERIALIZER = new Deserializer<Span>() {

		@Override
		public void configure(Map<String, ?> map, boolean bln) {
		}

		@Override
		public Span deserialize(String string, byte[] bytes) {
			if (bytes != null) {
				try {
					return Span.parseFrom(bytes);
				} catch (InvalidProtocolBufferException ex) {
					System.err.println(ex.getMessage());
				}
			}
			return null;
		}

		@Override
		public void close() {
		}
	};

	@Override
	public void configure(Map<String, ?> map, boolean bln) {
	}

	@Override
	public void close() {
	}

	@Override
	public Serializer<Span> serializer() {
		return SERIALIZER;
	}

	@Override
	public Deserializer<Span> deserializer() {
		return DESERIALIZER;
	}
}
