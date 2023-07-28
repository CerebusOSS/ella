import ella
import pyarrow as pa

def main(root):
    schema = ella.schema([
        ella.field("i", ella.int32, required=True, index='ascending'),
        ella.field("x", ella.float32, row_shape=(2, )),
        ella.field("y", ella.float32),
    ])

    fixed = pa.fixed_shape_tensor(pa.float32(), (2, ))

    with ella.engine(root) as rt:
        topic = rt.topic("point", schema)
        pb = topic.publish()
        for i in range(100):
            batch = pa.record_batch(
                [
                    pa.array([i]),
                    pa.array([[i * 0.5, i * 0.25]], type=fixed),
                    pa.array([i * 10.]),
                ],
                schema=pa.schema([
                    pa.field("i", pa.int32(), nullable=False),
                    pa.field("x", fixed),
                    pa.field("y", pa.float32()),
                ]),
            )

            pb.try_write(batch)


if __name__ == "__main__":
    main("file:///tmp/pyella")
