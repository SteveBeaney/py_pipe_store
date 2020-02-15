import pytest
import datetime
import math
from intermagnet import inter as im


def test_interfile_site():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/val20200117vmin.min.gz')
    assert ifile.get_site() == 'VAL', "test failed"


def test_interfile_rows():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/val20200117vmin.min.gz')
    assert ifile.get_rows() == 1440, "test failed"


def test_interfile_form():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/val20200117vmin.min.gz')
    assert ifile.get_form() == 'HDZF', "test failed"


def test_interfile_date():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/val20200117vmin.min.gz')
    assert ifile.get_date() == datetime.date(2020, 1, 17), "test failed"


def test_interfile_data():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/val20200117vmin.min.gz')
    assert math.fabs(ifile.get_data()['x'][4] - -10710.688921) < 0.0001, "test failed"


def test_interfile_site_u():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/ups20200102vmin.min.gz')
    assert ifile.get_site() == 'UPS', "test failed"


def test_interfile_rows_u():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/ups20200102vmin.min.gz')
    assert ifile.get_rows() == 1440, "test failed"


def test_interfile_form_u():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/ups20200102vmin.min.gz')
    assert ifile.get_form() == 'XYZF', "test failed"


def test_interfile_date_u():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/ups20200102vmin.min.gz')
    assert ifile.get_date() == datetime.date(2020, 1, 2), "test failed"


def test_interfile_data_u():
    ifile = im.Intermag_File('/home/steve/repos/py_pipe_store/testdata/ups20200102vmin.min.gz')
    assert math.fabs(ifile.get_data()['x'][4] - 15096.4) < 0.0001, "test failed"




