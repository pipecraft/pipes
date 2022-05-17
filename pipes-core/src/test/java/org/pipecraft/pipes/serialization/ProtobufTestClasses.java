/**
 * Serves for test purposes only.
 * Auto generated from the following proto definition:
 *
 * syntax = "proto3";
 *
 * message EventRecord {
 *   int32 id = 1;
 *   string description = 2;
 *   int32 timestamp = 3;
 *   repeated int32 user = 4;
 * }
 *
 */
package org.pipecraft.pipes.serialization;

public final class ProtobufTestClasses {
  private ProtobufTestClasses() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface EventRecordOrBuilder extends
      // @@protoc_insertion_point(interface_extends:EventRecord)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>int32 id = 1;</code>
     * @return The id.
     */
    int getId();

    /**
     * <code>string description = 2;</code>
     * @return The description.
     */
    java.lang.String getDescription();
    /**
     * <code>string description = 2;</code>
     * @return The bytes for description.
     */
    com.google.protobuf.ByteString
        getDescriptionBytes();

    /**
     * <code>int32 timestamp = 3;</code>
     * @return The timestamp.
     */
    int getTimestamp();

    /**
     * <code>repeated int32 user = 4;</code>
     * @return A list containing the user.
     */
    java.util.List<java.lang.Integer> getUserList();
    /**
     * <code>repeated int32 user = 4;</code>
     * @return The count of user.
     */
    int getUserCount();
    /**
     * <code>repeated int32 user = 4;</code>
     * @param index The index of the element to return.
     * @return The user at the given index.
     */
    int getUser(int index);
  }
  /**
   * Protobuf type {@code EventRecord}
   */
  public static final class EventRecord extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:EventRecord)
      EventRecordOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use EventRecord.newBuilder() to construct.
    private EventRecord(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private EventRecord() {
      description_ = "";
      user_ = emptyIntList();
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new EventRecord();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private EventRecord(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 8: {

              id_ = input.readInt32();
              break;
            }
            case 18: {
              java.lang.String s = input.readStringRequireUtf8();

              description_ = s;
              break;
            }
            case 24: {

              timestamp_ = input.readInt32();
              break;
            }
            case 32: {
              if (!((mutable_bitField0_ & 0x00000001) != 0)) {
                user_ = newIntList();
                mutable_bitField0_ |= 0x00000001;
              }
              user_.addInt(input.readInt32());
              break;
            }
            case 34: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000001) != 0) && input.getBytesUntilLimit() > 0) {
                user_ = newIntList();
                mutable_bitField0_ |= 0x00000001;
              }
              while (input.getBytesUntilLimit() > 0) {
                user_.addInt(input.readInt32());
              }
              input.popLimit(limit);
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (com.google.protobuf.UninitializedMessageException e) {
        throw e.asInvalidProtocolBufferException().setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000001) != 0)) {
          user_.makeImmutable(); // C
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return ProtobufTestClasses.internal_static_EventRecord_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return ProtobufTestClasses.internal_static_EventRecord_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              ProtobufTestClasses.EventRecord.class, ProtobufTestClasses.EventRecord.Builder.class);
    }

    public static final int ID_FIELD_NUMBER = 1;
    private int id_;
    /**
     * <code>int32 id = 1;</code>
     * @return The id.
     */
    @java.lang.Override
    public int getId() {
      return id_;
    }

    public static final int DESCRIPTION_FIELD_NUMBER = 2;
    private volatile java.lang.Object description_;
    /**
     * <code>string description = 2;</code>
     * @return The description.
     */
    @java.lang.Override
    public java.lang.String getDescription() {
      java.lang.Object ref = description_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        description_ = s;
        return s;
      }
    }
    /**
     * <code>string description = 2;</code>
     * @return The bytes for description.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString
        getDescriptionBytes() {
      java.lang.Object ref = description_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        description_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int TIMESTAMP_FIELD_NUMBER = 3;
    private int timestamp_;
    /**
     * <code>int32 timestamp = 3;</code>
     * @return The timestamp.
     */
    @java.lang.Override
    public int getTimestamp() {
      return timestamp_;
    }

    public static final int USER_FIELD_NUMBER = 4;
    private com.google.protobuf.Internal.IntList user_;
    /**
     * <code>repeated int32 user = 4;</code>
     * @return A list containing the user.
     */
    @java.lang.Override
    public java.util.List<java.lang.Integer>
        getUserList() {
      return user_;
    }
    /**
     * <code>repeated int32 user = 4;</code>
     * @return The count of user.
     */
    public int getUserCount() {
      return user_.size();
    }
    /**
     * <code>repeated int32 user = 4;</code>
     * @param index The index of the element to return.
     * @return The user at the given index.
     */
    public int getUser(int index) {
      return user_.getInt(index);
    }
    private int userMemoizedSerializedSize = -1;

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (id_ != 0) {
        output.writeInt32(1, id_);
      }
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(description_)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, description_);
      }
      if (timestamp_ != 0) {
        output.writeInt32(3, timestamp_);
      }
      if (getUserList().size() > 0) {
        output.writeUInt32NoTag(34);
        output.writeUInt32NoTag(userMemoizedSerializedSize);
      }
      for (int i = 0; i < user_.size(); i++) {
        output.writeInt32NoTag(user_.getInt(i));
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (id_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(1, id_);
      }
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(description_)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, description_);
      }
      if (timestamp_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(3, timestamp_);
      }
      {
        int dataSize = 0;
        for (int i = 0; i < user_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeInt32SizeNoTag(user_.getInt(i));
        }
        size += dataSize;
        if (!getUserList().isEmpty()) {
          size += 1;
          size += com.google.protobuf.CodedOutputStream
              .computeInt32SizeNoTag(dataSize);
        }
        userMemoizedSerializedSize = dataSize;
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof ProtobufTestClasses.EventRecord)) {
        return super.equals(obj);
      }
      ProtobufTestClasses.EventRecord other = (ProtobufTestClasses.EventRecord) obj;

      if (getId()
          != other.getId()) return false;
      if (!getDescription()
          .equals(other.getDescription())) return false;
      if (getTimestamp()
          != other.getTimestamp()) return false;
      if (!getUserList()
          .equals(other.getUserList())) return false;
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + ID_FIELD_NUMBER;
      hash = (53 * hash) + getId();
      hash = (37 * hash) + DESCRIPTION_FIELD_NUMBER;
      hash = (53 * hash) + getDescription().hashCode();
      hash = (37 * hash) + TIMESTAMP_FIELD_NUMBER;
      hash = (53 * hash) + getTimestamp();
      if (getUserCount() > 0) {
        hash = (37 * hash) + USER_FIELD_NUMBER;
        hash = (53 * hash) + getUserList().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static ProtobufTestClasses.EventRecord parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static ProtobufTestClasses.EventRecord parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static ProtobufTestClasses.EventRecord parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static ProtobufTestClasses.EventRecord parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static ProtobufTestClasses.EventRecord parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static ProtobufTestClasses.EventRecord parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static ProtobufTestClasses.EventRecord parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static ProtobufTestClasses.EventRecord parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static ProtobufTestClasses.EventRecord parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static ProtobufTestClasses.EventRecord parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static ProtobufTestClasses.EventRecord parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static ProtobufTestClasses.EventRecord parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(ProtobufTestClasses.EventRecord prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code EventRecord}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:EventRecord)
        ProtobufTestClasses.EventRecordOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return ProtobufTestClasses.internal_static_EventRecord_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return ProtobufTestClasses.internal_static_EventRecord_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                ProtobufTestClasses.EventRecord.class, ProtobufTestClasses.EventRecord.Builder.class);
      }

      // Construct using Test.EventRecord.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        id_ = 0;

        description_ = "";

        timestamp_ = 0;

        user_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return ProtobufTestClasses.internal_static_EventRecord_descriptor;
      }

      @java.lang.Override
      public ProtobufTestClasses.EventRecord getDefaultInstanceForType() {
        return ProtobufTestClasses.EventRecord.getDefaultInstance();
      }

      @java.lang.Override
      public ProtobufTestClasses.EventRecord build() {
        ProtobufTestClasses.EventRecord result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public ProtobufTestClasses.EventRecord buildPartial() {
        ProtobufTestClasses.EventRecord result = new ProtobufTestClasses.EventRecord(this);
        int from_bitField0_ = bitField0_;
        result.id_ = id_;
        result.description_ = description_;
        result.timestamp_ = timestamp_;
        if (((bitField0_ & 0x00000001) != 0)) {
          user_.makeImmutable();
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.user_ = user_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof ProtobufTestClasses.EventRecord) {
          return mergeFrom((ProtobufTestClasses.EventRecord)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(ProtobufTestClasses.EventRecord other) {
        if (other == ProtobufTestClasses.EventRecord.getDefaultInstance()) return this;
        if (other.getId() != 0) {
          setId(other.getId());
        }
        if (!other.getDescription().isEmpty()) {
          description_ = other.description_;
          onChanged();
        }
        if (other.getTimestamp() != 0) {
          setTimestamp(other.getTimestamp());
        }
        if (!other.user_.isEmpty()) {
          if (user_.isEmpty()) {
            user_ = other.user_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureUserIsMutable();
            user_.addAll(other.user_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        ProtobufTestClasses.EventRecord parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (ProtobufTestClasses.EventRecord) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private int id_ ;
      /**
       * <code>int32 id = 1;</code>
       * @return The id.
       */
      @java.lang.Override
      public int getId() {
        return id_;
      }
      /**
       * <code>int32 id = 1;</code>
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(int value) {
        
        id_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>int32 id = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearId() {
        
        id_ = 0;
        onChanged();
        return this;
      }

      private java.lang.Object description_ = "";
      /**
       * <code>string description = 2;</code>
       * @return The description.
       */
      public java.lang.String getDescription() {
        java.lang.Object ref = description_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          description_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string description = 2;</code>
       * @return The bytes for description.
       */
      public com.google.protobuf.ByteString
          getDescriptionBytes() {
        java.lang.Object ref = description_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          description_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string description = 2;</code>
       * @param value The description to set.
       * @return This builder for chaining.
       */
      public Builder setDescription(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        description_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>string description = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearDescription() {
        
        description_ = getDefaultInstance().getDescription();
        onChanged();
        return this;
      }
      /**
       * <code>string description = 2;</code>
       * @param value The bytes for description to set.
       * @return This builder for chaining.
       */
      public Builder setDescriptionBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        description_ = value;
        onChanged();
        return this;
      }

      private int timestamp_ ;
      /**
       * <code>int32 timestamp = 3;</code>
       * @return The timestamp.
       */
      @java.lang.Override
      public int getTimestamp() {
        return timestamp_;
      }
      /**
       * <code>int32 timestamp = 3;</code>
       * @param value The timestamp to set.
       * @return This builder for chaining.
       */
      public Builder setTimestamp(int value) {
        
        timestamp_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>int32 timestamp = 3;</code>
       * @return This builder for chaining.
       */
      public Builder clearTimestamp() {
        
        timestamp_ = 0;
        onChanged();
        return this;
      }

      private com.google.protobuf.Internal.IntList user_ = emptyIntList();
      private void ensureUserIsMutable() {
        if (!((bitField0_ & 0x00000001) != 0)) {
          user_ = mutableCopy(user_);
          bitField0_ |= 0x00000001;
         }
      }
      /**
       * <code>repeated int32 user = 4;</code>
       * @return A list containing the user.
       */
      public java.util.List<java.lang.Integer>
          getUserList() {
        return ((bitField0_ & 0x00000001) != 0) ?
                 java.util.Collections.unmodifiableList(user_) : user_;
      }
      /**
       * <code>repeated int32 user = 4;</code>
       * @return The count of user.
       */
      public int getUserCount() {
        return user_.size();
      }
      /**
       * <code>repeated int32 user = 4;</code>
       * @param index The index of the element to return.
       * @return The user at the given index.
       */
      public int getUser(int index) {
        return user_.getInt(index);
      }
      /**
       * <code>repeated int32 user = 4;</code>
       * @param index The index to set the value at.
       * @param value The user to set.
       * @return This builder for chaining.
       */
      public Builder setUser(
          int index, int value) {
        ensureUserIsMutable();
        user_.setInt(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int32 user = 4;</code>
       * @param value The user to add.
       * @return This builder for chaining.
       */
      public Builder addUser(int value) {
        ensureUserIsMutable();
        user_.addInt(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int32 user = 4;</code>
       * @param values The user to add.
       * @return This builder for chaining.
       */
      public Builder addAllUser(
          java.lang.Iterable<? extends java.lang.Integer> values) {
        ensureUserIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, user_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int32 user = 4;</code>
       * @return This builder for chaining.
       */
      public Builder clearUser() {
        user_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:EventRecord)
    }

    // @@protoc_insertion_point(class_scope:EventRecord)
    private static final ProtobufTestClasses.EventRecord DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new ProtobufTestClasses.EventRecord();
    }

    public static ProtobufTestClasses.EventRecord getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<EventRecord>
        PARSER = new com.google.protobuf.AbstractParser<EventRecord>() {
      @java.lang.Override
      public EventRecord parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new EventRecord(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<EventRecord> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<EventRecord> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public ProtobufTestClasses.EventRecord getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_EventRecord_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_EventRecord_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\nTest.proto\"O\n\013EventRecord\022\n\n\002id\030\001 \001(\005\022" +
      "\023\n\013description\030\002 \001(\t\022\021\n\ttimestamp\030\003 \001(\005\022" +
      "\014\n\004user\030\004 \003(\005b\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_EventRecord_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_EventRecord_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_EventRecord_descriptor,
        new java.lang.String[] { "Id", "Description", "Timestamp", "User", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
