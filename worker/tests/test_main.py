from agent.main import main


def test_main_returns_error_and_logs_on_invalid_config(monkeypatch, capsys):
    monkeypatch.setenv("ORIONQUEUE_LOG_LEVEL", "not-a-level")

    exit_code = main([])

    assert exit_code == 1
    err = capsys.readouterr().err
    assert "invalid configuration" in err
    assert "not-a-level" in err
