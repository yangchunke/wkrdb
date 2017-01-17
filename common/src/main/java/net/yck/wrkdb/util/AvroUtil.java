package net.yck.wrkdb.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;

public final class AvroUtil {
    public static String toJson(GenericRecord rec, Schema schema) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos);
        doWrite(rec, schema, encoder);
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    public static GenericRecord fromJson(String json, Schema schema) throws IOException {
        final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
        return doRead(schema, decoder);
    }

    public static byte[] toBytes(GenericRecord rec, Schema schema) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        doWrite(rec, schema, encoder);
        return outputStream.toByteArray();
    }

    public static GenericRecord fromBytes(byte[] bytes, Schema schema) throws IOException {
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return doRead(schema, decoder);
    }

    private static GenericRecord doRead(Schema schema, final Decoder decoder) throws IOException {
        final DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        return reader.read(null, decoder);
    }

    private static void doWrite(GenericRecord rec, Schema schema, final Encoder encoder) throws IOException {
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(rec, encoder);
        encoder.flush();
    }
}
