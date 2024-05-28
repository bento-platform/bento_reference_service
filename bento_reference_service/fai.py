__all__ = ["parse_fai"]


def parse_fai(fai_data: bytes) -> dict[str, tuple[int, int, int, int]]:
    res: dict[str, tuple[int, int, int, int]] = {}

    for record in fai_data.split(b"\n"):
        if not record:  # trailing newline or whatever
            continue

        row = record.split(b"\t")
        if len(row) != 5:
            raise ValueError(f"Invalid FAI record: {record.decode('ascii')}")

        # FAI record: contig, (num bases, byte index, bases per line, bytes per line)
        res[row[0].decode("ascii")] = (int(row[1]), int(row[2]), int(row[3]), int(row[4]))

    return res
