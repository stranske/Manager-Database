"""A plausible assertion receipt cannot override failed replay phase controls."""

import xml.etree.ElementTree as ET

import pytest

from tests.test_dashboard_read_failure_replay import NAME, replay, report


@pytest.mark.parametrize("phase,outcome", [("red", "failure"), ("green", "")])
@pytest.mark.parametrize("code", [2, 5, -9], ids=["interrupted", "no-tests", "killed"])
def test_replay_rejects_unsuccessful_process_even_with_valid_receipt(
    tmp_path, phase, outcome, code
):
    with pytest.raises(ValueError, match="Wrong phase exit"):
        replay.validate_junit(report(tmp_path, outcome=outcome), NAME, phase, code)


@pytest.mark.parametrize("phase,outcome,code", [("red", "failure", 0), ("green", "", 1)])
def test_replay_rejects_reversed_exit_status(tmp_path, phase, outcome, code):
    with pytest.raises(ValueError, match="Wrong phase exit"):
        replay.validate_junit(report(tmp_path, outcome=outcome), NAME, phase, code)


@pytest.mark.parametrize("phase,outcome,code", [("red", "failure", 1), ("green", "", 0)])
@pytest.mark.parametrize("field", ["tests", "failures", "errors", "skipped"])
def test_replay_rejects_suite_counts_that_disagree_with_case(tmp_path, phase, outcome, code, field):
    path = report(tmp_path, outcome=outcome)
    tree = ET.parse(path)
    suite = tree.getroot().find("testsuite")
    suite.set(field, str(int(suite.get(field)) + 1))
    tree.write(path)

    with pytest.raises(ValueError, match="Wrong suite outcomes"):
        replay.validate_junit(path, NAME, phase, code)


@pytest.mark.parametrize("phase,outcome,code", [("red", "failure", 1), ("green", "", 0)])
def test_replay_rejects_case_outside_counted_suite(tmp_path, phase, outcome, code):
    path = report(tmp_path, outcome=outcome)
    tree = ET.parse(path)
    root = tree.getroot()
    suite = root.find("testsuite")
    case = suite.find("testcase")
    suite.remove(case)
    root.append(case)
    tree.write(path)

    with pytest.raises(ValueError, match="Wrong test identity"):
        replay.validate_junit(path, NAME, phase, code)


@pytest.mark.parametrize("outcome", ["failure", "error", "skipped"])
def test_replay_rejects_outcomes_hidden_outside_green_case(tmp_path, outcome):
    path = report(tmp_path, outcome="")
    tree = ET.parse(path)
    ET.SubElement(tree.getroot(), outcome)
    tree.write(path)

    with pytest.raises(ValueError, match="Error, skip or misplaced outcome"):
        replay.validate_junit(path, NAME, "green", 0)


def test_replay_rejects_red_failure_outside_selected_case(tmp_path):
    path = report(tmp_path)
    tree = ET.parse(path)
    root = tree.getroot()
    case = root.find("testsuite/testcase")
    failure = case.find("failure")
    case.remove(failure)
    root.append(failure)
    tree.write(path)

    with pytest.raises(ValueError, match="Error, skip or misplaced outcome"):
        replay.validate_junit(path, NAME, "red", 1)
