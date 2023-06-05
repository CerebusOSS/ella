import synapse
import pyarrow as pa

def main(root):
    schema = synapse.schema([
        synapse.field("i", synapse.int32, required=True, index='ascending'),
        synapse.field("x", synapse.float32),
        synapse.field("y", synapse.float32),
    ])

    with synapse.runtime(root) as rt:
        topic = rt.topic("point", schema)
        pb = topic.publish()
        for i in range(100):
            batch = pa.record_batch(
                [
                    pa.array([i]),
                    pa.array([i * 0.5]),
                    pa.array([i * 10.]),
                ],
                schema=schema.arrow_schema,
            )
            pb.try_write(batch)


if __name__ == "__main__":
    main("file:///tmp/pysynapse")
