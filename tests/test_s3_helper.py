from docling_jobkit.connectors.s3_helper import strip_prefix_postfix


def test_strip_prefix_postfix():
    in_set = {"mypath/json/file_1.json", "mypath/json/file_2.json"}
    out_set = strip_prefix_postfix(in_set, prefix="mypath/json/", extension=".json")

    assert len(in_set) == len(out_set)
    assert out_set == {"file_1", "file_2"}
