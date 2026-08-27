import asyncio

import main


class FakeFuture:
    def __init__(self, value):
        self.value = value

    def result(self):
        return self.value


class FakePublisher:
    def __init__(self, project_id):
        self.project_id = project_id
        self.published = []

    def topic_path(self, project, topic):
        self.published.append((project, topic))
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic_path, data=b""):
        self.published.append((topic_path, data))
        return FakeFuture("fake-message-id")


def test_get_project_id_uses_env_override(monkeypatch):
    monkeypatch.setattr(main, "PROJECT_ID", "env-project", raising=False)
    monkeypatch.setattr(main, "get_bq_client", lambda: (_ for _ in ()).throw(AssertionError("should not call BigQuery client")))

    assert main.get_project_id() == "env-project"


def test_get_project_id_uses_bq_client_when_env_missing(monkeypatch):
    monkeypatch.setattr(main, "PROJECT_ID", None, raising=False)

    class FakeClient:
        project = "bq-project"

    monkeypatch.setattr(main, "get_bq_client", lambda: FakeClient())

    assert main.get_project_id() == "bq-project"


def test_trigger_only_publishes_for_master_table(monkeypatch):
    async def run_test():
        monkeypatch.setattr(main, "PROJECT_ID", "test-project", raising=False)
        monkeypatch.setattr(main, "TOPIC_ID", "test-topic", raising=False)

        async def fake_request_json():
            return {"bucket": "bucket-name", "name": "mprop_master.csv"}

        class FakeRequest:
            async def json(self):
                return await fake_request_json()

        fake_client = FakePublisher("test-project")
        monkeypatch.setattr(main, "load_gcs_file_into_bigquery", lambda bucket, file_name: (42, "mprop_master"))
        monkeypatch.setattr(main.pubsub_v1, "PublisherClient", lambda: fake_client)

        response = await main.gcs_to_bigquery_trigger(FakeRequest())
        assert response == {"status": "success", "output_rows": 42}
        assert fake_client.published[0] == ("test-project", "test-topic")

    asyncio.run(run_test())


def test_trigger_skips_publish_for_non_master_table(monkeypatch):
    async def run_test():
        monkeypatch.setattr(main, "PROJECT_ID", "test-project", raising=False)
        monkeypatch.setattr(main, "TOPIC_ID", "test-topic", raising=False)

        async def fake_request_json():
            return {"bucket": "bucket-name", "name": "other_table.csv"}

        class FakeRequest:
            async def json(self):
                return await fake_request_json()

        class FakePublisher:
            def topic_path(self, project, topic):
                raise AssertionError("should not publish")

        monkeypatch.setattr(main, "load_gcs_file_into_bigquery", lambda bucket, file_name: (7, "other_table"))
        monkeypatch.setattr(main.pubsub_v1, "PublisherClient", FakePublisher)

        response = await main.gcs_to_bigquery_trigger(FakeRequest())
        assert response == {"status": "success", "output_rows": 7}

    asyncio.run(run_test())
