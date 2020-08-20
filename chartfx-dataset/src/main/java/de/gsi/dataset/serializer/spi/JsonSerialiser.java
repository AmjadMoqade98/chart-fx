package de.gsi.dataset.serializer.spi;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;

import com.jsoniter.JsonIterator;
import com.jsoniter.JsonIteratorPool;
import com.jsoniter.any.Any;
import com.jsoniter.extra.PreciseFloatSupport;
import com.jsoniter.output.EncodingMode;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.DecodingMode;
import com.jsoniter.spi.JsonException;

import de.gsi.dataset.serializer.DataType;
import de.gsi.dataset.serializer.FieldDescription;
import de.gsi.dataset.serializer.FieldSerialiser;
import de.gsi.dataset.serializer.IoBuffer;
import de.gsi.dataset.serializer.IoSerialiser;
import de.gsi.dataset.utils.ByteBufferOutputStream;

public class JsonSerialiser implements IoSerialiser {
    private static final byte BRACKET_OPEN = '{';
    private static final byte BRACKET_CLOSE = '{';
    public static final String NOT_A_JSON_COMPATIBLE_PROTOCOL = "Not a JSON compatible protocol";
    private IoBuffer buffer;
    private boolean putFieldMetaData = true;
    private Any root = null;
    private Any tempRoot = null;
    private WireDataFieldDescription parent;
    private WireDataFieldDescription lastFieldHeader;
    private String queryFieldName = null;
    private final StringBuilder builder = new StringBuilder(100);

    /**
     * @param buffer the backing IoBuffer (see e.g. {@link de.gsi.dataset.serializer.spi.FastByteBuffer} or{@link de.gsi.dataset.serializer.spi.ByteBuffer}
     */
    public JsonSerialiser(final IoBuffer buffer) {
        super();
        this.buffer = buffer;
    }

    @Override
    public ProtocolInfo checkHeaderInfo() {
        // make coarse check (ie. check if first non-null character is a '{' bracket
        int count = buffer.position();
        while (buffer.getByte(count) != BRACKET_OPEN && (buffer.getByte(count) == 0 || buffer.getByte(count) == ' ' || buffer.getByte(count) == '\t' || buffer.getByte(count) == '\n')) {
            count++;
        }
        if (buffer.getByte(count) != BRACKET_OPEN) {
            throw new IllegalStateException(NOT_A_JSON_COMPATIBLE_PROTOCOL);
        }
        JsonIterator iter = JsonIteratorPool.borrowJsonIterator();
        iter.reset(buffer.elements(), 0, buffer.limit());

        try {
            tempRoot = root = iter.readAny();
        } catch (IOException e) {
            e.printStackTrace(); //TODO: remove for production use
            throw new IllegalStateException(NOT_A_JSON_COMPATIBLE_PROTOCOL, e);
        }

        final WireDataFieldDescription headerStartField = new WireDataFieldDescription(this, null, "JSON_ROOT".hashCode(), "JSON_ROOT", DataType.OTHER, buffer.position(), count - 1, -1);
        final ProtocolInfo header = new ProtocolInfo(this, headerStartField, JsonSerialiser.class.getCanonicalName(), (byte) 1, (byte) 0, (byte) 0);
        parent = lastFieldHeader = headerStartField;
        queryFieldName = "JSON_ROOT";
        return header;
    }

    @Override
    public void setQueryFieldName(final String fieldName, final int dataStartPosition) {
        if (fieldName == null || fieldName.isBlank()) {
            throw new IllegalArgumentException("fieldName must not be null or blank: " + fieldName);
        }
        if (root == null) {
            throw new IllegalArgumentException("JSON Any root hasn't been analysed/parsed yet");
        }
        this.queryFieldName = fieldName;
        // buffer.position(dataStartPosition); // N.B. not needed at this time
    }

    @Override
    public int[] getArraySizeDescriptor() {
        return new int[0];
    }

    @Override
    public boolean getBoolean() {
        return tempRoot.get(queryFieldName).toBoolean();
    }

    @Override
    public boolean[] getBooleanArray(final boolean[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(boolean[].class);
    }

    @Override
    public IoBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void setBuffer(final IoBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte getByte() {
        return (byte) tempRoot.get(queryFieldName).toInt();
    }

    @Override
    public byte[] getByteArray(final byte[] dst, final int length) {
        return new byte[0];
    }

    @Override
    public char getChar() {
        return (char) tempRoot.get(queryFieldName).toInt();
    }

    @Override
    public char[] getCharArray(final char[] dst, final int length) {
        return tempRoot.get(queryFieldName).as(char[].class);
    }

    @Override
    public <E> Collection<E> getCollection(final Collection<E> collection, final BiFunction<Type, Type[], FieldSerialiser<E>> serialiserLookup) {
        return null;
    }

    @Override
    public <E> E getCustomData(final FieldSerialiser<E> serialiser) {
        return null;
    }

    @Override
    public double getDouble() {
        return tempRoot.get(queryFieldName).toDouble();
    }

    @Override
    public double[] getDoubleArray(final double[] dst, final int length) {
        return new double[0];
    }

    @Override
    public <E extends Enum<E>> Enum<E> getEnum(final Enum<E> enumeration) {
        return null;
    }

    @Override
    public String getEnumTypeList() {
        return null;
    }

    @Override
    public WireDataFieldDescription getFieldHeader() {
        return null;
    }

    @Override
    public float getFloat() {
        return tempRoot.get(queryFieldName).toFloat();
    }

    @Override
    public float[] getFloatArray(final float[] dst, final int length) {
        return new float[0];
    }

    @Override
    public int getInt() {
        return tempRoot.get(queryFieldName).toInt();
    }

    @Override
    public int[] getIntArray(final int[] dst, final int length) {
        return new int[0];
    }

    @Override
    public <E> List<E> getList(final List<E> collection, final BiFunction<Type, Type[], FieldSerialiser<E>> serialiserLookup) {
        return null;
    }

    @Override
    public long getLong() {
        return tempRoot.get(queryFieldName).toLong();
    }

    @Override
    public long[] getLongArray(final long[] dst, final int length) {
        return new long[0];
    }

    @Override
    public <K, V, E> Map<K, V> getMap(final Map<K, V> map, final BiFunction<Type, Type[], FieldSerialiser<E>> serialiserLookup) {
        return null;
    }

    public WireDataFieldDescription getParent() {
        return parent;
    }

    @Override
    public <E> Queue<E> getQueue(final Queue<E> collection, final BiFunction<Type, Type[], FieldSerialiser<E>> serialiserLookup) {
        return null;
    }

    @Override
    public <E> Set<E> getSet(final Set<E> collection, final BiFunction<Type, Type[], FieldSerialiser<E>> serialiserLookup) {
        return null;
    }

    @Override
    public short getShort() {
        return (short) tempRoot.get(queryFieldName).toLong();
    }

    @Override
    public short[] getShortArray(final short[] dst, final int length) {
        return new short[0];
    }

    @Override
    public String getString() {
        return tempRoot.get(queryFieldName).toString();
    }

    @Override
    public String[] getStringArray(final String[] dst, final int length) {
        return new String[0];
    }

    @Override
    public String getStringISO8859() {
        return tempRoot.get(queryFieldName).toString();
    }

    @Override
    public boolean isPutFieldMetaData() {
        return putFieldMetaData;
    }

    @Override
    public void setPutFieldMetaData(final boolean putFieldMetaData) {
        this.putFieldMetaData = putFieldMetaData;
    }

    @Override
    public WireDataFieldDescription parseIoStream(final boolean readHeader) {
        JsonIterator iter = JsonIteratorPool.borrowJsonIterator();
        iter.reset(buffer.elements(), 0, buffer.limit());

        try {
            tempRoot = root = iter.readAny();
        } catch (IOException e) {
            e.printStackTrace(); //TODO: remove for production use
            throw new IllegalStateException(NOT_A_JSON_COMPATIBLE_PROTOCOL, e);
        }
        // TODO: continue here populating and generating WireDataFieldDescription tree
        return null;
    }

    @Override
    public <E> void put(final FieldDescription fieldDescription, final Collection<E> collection, final Type valueType, final BiFunction<Type, Type[], FieldSerialiser<E>> serialiserLookup) {
        put(fieldDescription.getFieldName(), collection, valueType, serialiserLookup);
    }

    @Override
    public void put(final FieldDescription fieldDescription, final Enum<?> enumeration) {
    }

    @Override
    public <K, V, E> void put(final FieldDescription fieldDescription, final Map<K, V> map, final Type keyType, final Type valueType, final BiFunction<Type, Type[], FieldSerialiser<E>> serialiserLookup) {
    }

    @Override
    public <E> void put(final String fieldName, final Collection<E> collection, final Type valueType, final BiFunction<Type, Type[], FieldSerialiser<E>> serialiserLookup) {
        builder.append("{\"").append(fieldName).append("\", ").append("{");
        if (collection == null || collection.isEmpty()) {
            builder.append('}').append('}');
            return;
        }
        final Iterator<E> iter = collection.iterator();
        builder.append(iter.next().toString());
        E element;
        while ((element = iter.next()) != null) {
            builder.append(", ").append(element.toString());
        }
        builder.append('}').append('}');
    }

    @Override
    public void put(final String fieldName, final Enum<?> enumeration) {
    }

    @Override
    public <K, V, E> void put(final String fieldName, final Map<K, V> map, final Type keyType, final Type valueType, final BiFunction<Type, Type[], FieldSerialiser<E>> serialiserLookup) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final boolean value) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final boolean[] values, final int n) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final boolean[] values, final int[] dims) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final byte value) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final byte[] values, final int n) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final byte[] values, final int[] dims) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final char value) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final char[] values, final int n) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final char[] values, final int[] dims) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final double value) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final double[] values, final int n) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final double[] values, final int[] dims) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final float value) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final float[] values, final int n) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final float[] values, final int[] dims) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final int value) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final int[] values, final int n) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final int[] values, final int[] dims) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final long value) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final long[] values, final int n) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final long[] values, final int[] dims) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final short value) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final short[] values, final int n) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final short[] values, final int[] dims) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final String string) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final String[] values, final int n) {
    }

    @Override
    public void put(final FieldDescription fieldDescription, final String[] values, final int[] dims) {
    }

    @Override
    public void put(final String fieldName, final boolean value) {
    }

    @Override
    public void put(final String fieldName, final boolean[] values, final int n) {
        builder.append("{\"").append(fieldName).append("\", ").append("{");
        if (values == null || values.length <= 0) {
            builder.append('}').append('}');
            return;
        }
        builder.append(values[0]);
        final int nElements = n >= 0 ? Math.min(n, values.length) : values.length;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ").append(values[i]);
        }
        builder.append('}').append('}');
    }

    @Override
    public void put(final String fieldName, final boolean[] values, final int[] dims) {
    }

    @Override
    public void put(final String fieldName, final byte value) {
    }

    @Override
    public void put(final String fieldName, final byte[] values, final int n) {
        builder.append("{\"").append(fieldName).append("\", ").append("{");
        if (values == null || values.length <= 0) {
            builder.append('}').append('}');
            return;
        }
        builder.append(values[0]);
        final int valuesSize = values == null ? 0 : values.length;
        final int nElements = n >= 0 ? Math.min(n, valuesSize) : valuesSize;
        for (int i = 1; i < nElements; i++) {
            builder.append(", ")
                    .append(values[i]);
        }
        builder.append('}').append('}');
    }

    @Override
    public void put(final String fieldName, final byte[] values, final int[] dims) {
    }

    @Override
    public void put(final String fieldName, final char value) {
    }

    @Override
    public void put(final String fieldName, final char[] values, final int n) {
    }

    @Override
    public void put(final String fieldName, final char[] values, final int[] dims) {
    }

    @Override
    public void put(final String fieldName, final double value) {
    }

    @Override
    public void put(final String fieldName, final double[] values, final int n) {
    }

    @Override
    public void put(final String fieldName, final double[] values, final int[] dims) {
    }

    @Override
    public void put(final String fieldName, final float value) {
    }

    @Override
    public void put(final String fieldName, final float[] values, final int n) {
    }

    @Override
    public void put(final String fieldName, final float[] values, final int[] dims) {
    }

    @Override
    public void put(final String fieldName, final int value) {
    }

    @Override
    public void put(final String fieldName, final int[] values, final int n) {
    }

    @Override
    public void put(final String fieldName, final int[] values, final int[] dims) {
    }

    @Override
    public void put(final String fieldName, final long value) {
    }

    @Override
    public void put(final String fieldName, final long[] values, final int n) {
    }

    @Override
    public void put(final String fieldName, final long[] values, final int[] dims) {
    }

    @Override
    public void put(final String fieldName, final short value) {
    }

    @Override
    public void put(final String fieldName, final short[] values, final int n) {
    }

    @Override
    public void put(final String fieldName, final short[] values, final int[] dims) {
    }

    @Override
    public void put(final String fieldName, final String string) {
    }

    @Override
    public void put(final String fieldName, final String[] values, final int n) {
    }

    @Override
    public void put(final String fieldName, final String[] values, final int[] dims) {
    }

    @Override
    public int putArraySizeDescriptor(final int n) {
        return 0;
    }

    @Override
    public int putArraySizeDescriptor(final int[] dims) {
        return 0;
    }

    @Override
    public <E> WireDataFieldDescription putCustomData(final FieldDescription fieldDescription, final E obj, final Class<? extends E> type, final FieldSerialiser<E> serialiser) {
        return null;
    }

    @Override
    public void putEndMarker(final FieldDescription fieldDescription) {
        builder.append(BRACKET_CLOSE);
    }

    @Override
    public WireDataFieldDescription putFieldHeader(final String fieldName, final DataType dataType) {
        lastFieldHeader = new WireDataFieldDescription(this, parent, fieldName.hashCode(), fieldName, dataType, -1, 1, -1);
        queryFieldName = fieldName;
        return lastFieldHeader;
    }

    @Override
    public void putHeaderInfo(final FieldDescription... field) {
        // not needed
    }

    @Override
    public void putStartMarker(final FieldDescription fieldDescription) {
        builder.append(BRACKET_OPEN);
    }

    @Override
    public void updateDataEndMarker(final WireDataFieldDescription fieldHeader) {
        // not needed
    }

    public void serialiseObject(final Object obj) {
        if (obj == null) {
            // serialise null object
            builder.setLength(0);
            builder.append(BRACKET_OPEN).append(BRACKET_CLOSE);
            byte[] bytes = getSerialisedString().getBytes(Charset.defaultCharset());
            System.arraycopy(bytes, 0, buffer.elements(), buffer.position(), bytes.length);
            buffer.position(bytes.length);
            return;
        }
        final ByteBufferOutputStream byteOutputStream = new ByteBufferOutputStream(java.nio.ByteBuffer.wrap(buffer.elements()), false);
        JsonStream.setMode(EncodingMode.REFLECTION_MODE);
        JsonIterator.setMode(DecodingMode.REFLECTION_MODE);

        try {
            PreciseFloatSupport.enable();
        } catch (JsonException e) {
            // swallow subsequent enabling exceptions (function is guarded and supposed to be called only once)
        }

        byteOutputStream.position(buffer.position());
        JsonStream.serialize(obj, byteOutputStream);
        buffer.position(byteOutputStream.position());
    }

    public Object deserialiseObject(final Object obj) {
        final JsonIterator iter = JsonIterator.parse(buffer.elements(), 0, buffer.limit());
        try {
            return iter.read(obj);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public String getSerialisedString() {
        return buffer.toString();
    }
}
