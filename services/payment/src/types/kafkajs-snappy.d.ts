declare module 'kafkajs-snappy' {
  const SnappyCodec: () => {
    compress(encoder: { buffer: Buffer }): Promise<Buffer>;
    decompress(buffer: Buffer): Promise<Buffer>;
  };
  export = SnappyCodec;
}
