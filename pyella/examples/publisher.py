import ella
import numpy as np


def main(root):
    with ella.open(root, create=True) as rt:
        a = rt.tables.get_or_create(
            "a", ella.topic(ella.column("x", ella.float32, row_shape=(10, 10)))
        )
        with a.publish() as pb:
            for _ in range(10):
                pb.write(np.random.rand(10, 10).astype(np.float32))

        for df in rt.query("SELECT * FROM a"):
            print(df.to_arrow())


if __name__ == "__main__":
    main("/tmp/ella")
