import contextlib
import os
import shutil
import sys
import tempfile
import unittest
from copy import deepcopy
from unittest.mock import patch

import numpy as np
import pandas
import pytest
from packaging.version import Version

import ray
from ray import tune
from ray.air.constants import TRAINING_ITERATION
from ray.tune.search import ConcurrencyLimiter


def _invalid_objective(config):
    metric = "report"

    if config[metric] > 4:
        tune.report({"_metric": float("inf")})
    elif config[metric] > 3:
        tune.report({"_metric": float("-inf")})
    elif config[metric] > 2:
        tune.report({"_metric": np.nan})
    else:
        tune.report({"_metric": float(config[metric]) or 0.1})


def _multi_objective(config):
    tune.report(dict(a=config["a"] * 100, b=config["b"] * -100, c=config["c"]))


def _dummy_objective(config):
    tune.report(dict(metric=config["report"]))


class InvalidValuesTest(unittest.TestCase):
    """
    Test searcher handling of invalid values (NaN, -inf, inf).
    Implicitly tests automatic config conversion and default (anonymous)
    mode handling.
    Also tests that searcher save doesn't throw any errors during
    experiment checkpointing.
    """

    def setUp(self):
        self.config = {"report": tune.uniform(0.0, 5.0), "list": [1, 2, 3], "num": 4}

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4, num_gpus=0, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def assertCorrectExperimentOutput(self, analysis):
        best_trial = analysis.best_trial
        self.assertLessEqual(best_trial.config["report"], 2.0)
        # Make sure that constant parameters aren't lost
        # Hyperopt converts lists to tuples, so check for either
        self.assertIn(best_trial.config["list"], ([1, 2, 3], (1, 2, 3)))
        self.assertEqual(best_trial.config["num"], 4)

    @contextlib.contextmanager
    def check_searcher_checkpoint_errors_scope(self):
        buffer = []
        from ray.tune.execution.tune_controller import logger

        with patch.object(logger, "warning", lambda x: buffer.append(x)):
            yield

        assert not any(
            "Experiment state snapshotting failed: Can't pickle local object" in x
            for x in buffer
        ), "Searcher checkpointing failed (unable to serialize)."

    def testAxManualSetup(self):
        from ax.service.ax_client import AxClient

        from ray.tune.search.ax import AxSearch

        config = self.config.copy()
        config["mixed_list"] = [1, tune.uniform(2, 3), 4]
        converted_config = AxSearch.convert_search_space(config)
        # At least one nan, inf, -inf and float
        client = AxClient(random_seed=4321)
        client.create_experiment(
            parameters=converted_config, objective_name="_metric", minimize=False
        )
        searcher = AxSearch(ax_client=client)

        out = tune.run(
            _invalid_objective,
            search_alg=searcher,
            metric="_metric",
            mode="max",
            num_samples=4,
            reuse_actors=False,
        )
        self.assertCorrectExperimentOutput(out)
        self.assertEqual(out.best_trial.config["mixed_list"][0], 1)
        self.assertGreaterEqual(out.best_trial.config["mixed_list"][1], 2)
        self.assertLess(out.best_trial.config["mixed_list"][1], 3)
        self.assertEqual(out.best_trial.config["mixed_list"][2], 4)

    def testAx(self):
        from ray.tune.search.ax import AxSearch

        searcher = ConcurrencyLimiter(AxSearch(random_seed=4321), max_concurrent=2)

        with self.check_searcher_checkpoint_errors_scope():
            # Make sure enough samples are used so that Ax actually fits a model
            # for config suggestion
            out = tune.run(
                _invalid_objective,
                search_alg=searcher,
                metric="_metric",
                mode="max",
                num_samples=16,
                reuse_actors=False,
                config=self.config,
            )

        self.assertCorrectExperimentOutput(out)

    def testBayesOpt(self):
        from ray.tune.search.bayesopt import BayesOptSearch

        with self.check_searcher_checkpoint_errors_scope():
            out = tune.run(
                _invalid_objective,
                # At least one nan, inf, -inf and float
                search_alg=BayesOptSearch(random_state=1234),
                config=self.config,
                metric="_metric",
                mode="max",
                num_samples=8,
                reuse_actors=False,
            )
        self.assertCorrectExperimentOutput(out)

    @pytest.mark.skipif(
        sys.version_info >= (3, 12),
        reason="BOHB not yet supported for python 3.12+",
    )
    def testBOHB(self):
        from ray.tune.search.bohb import TuneBOHB

        with self.check_searcher_checkpoint_errors_scope():
            out = tune.run(
                _invalid_objective,
                search_alg=TuneBOHB(seed=1000),
                config=self.config,
                metric="_metric",
                mode="max",
                num_samples=8,
                reuse_actors=False,
            )
        self.assertCorrectExperimentOutput(out)

    @pytest.mark.skipif(
        sys.version_info >= (3, 12), reason="HEBO doesn't support py312"
    )
    def testHEBO(self):
        if Version(pandas.__version__) >= Version("2.0.0"):
            pytest.skip("HEBO does not support pandas>=2.0.0")

        from ray.tune.search.hebo import HEBOSearch

        with self.check_searcher_checkpoint_errors_scope():
            out = tune.run(
                _invalid_objective,
                # At least one nan, inf, -inf and float
                search_alg=HEBOSearch(random_state_seed=123),
                config=self.config,
                metric="_metric",
                mode="max",
                num_samples=8,
                reuse_actors=False,
            )
        self.assertCorrectExperimentOutput(out)

    def testHyperopt(self):
        from ray.tune.search.hyperopt import HyperOptSearch

        with self.check_searcher_checkpoint_errors_scope():
            out = tune.run(
                _invalid_objective,
                # At least one nan, inf, -inf and float
                search_alg=HyperOptSearch(random_state_seed=1234),
                config=self.config,
                metric="_metric",
                mode="max",
                num_samples=8,
                reuse_actors=False,
            )
        self.assertCorrectExperimentOutput(out)

    def testNevergrad(self):
        import nevergrad as ng

        from ray.tune.search.nevergrad import NevergradSearch

        np.random.seed(2020)  # At least one nan, inf, -inf and float

        with self.check_searcher_checkpoint_errors_scope():
            out = tune.run(
                _invalid_objective,
                search_alg=NevergradSearch(optimizer=ng.optimizers.RandomSearch),
                config=self.config,
                mode="max",
                num_samples=16,
                reuse_actors=False,
            )
        self.assertCorrectExperimentOutput(out)

    def testNevergradWithRequiredOptimizerKwargs(self):
        import nevergrad as ng

        from ray.tune.search.nevergrad import NevergradSearch

        NevergradSearch(optimizer=ng.optimizers.CM, optimizer_kwargs=dict(budget=16))

    def testOptuna(self):
        from optuna.samplers import RandomSampler

        from ray.tune.search.optuna import OptunaSearch

        np.random.seed(1000)  # At least one nan, inf, -inf and float

        with self.check_searcher_checkpoint_errors_scope():
            out = tune.run(
                _invalid_objective,
                search_alg=OptunaSearch(sampler=RandomSampler(seed=1234), storage=None),
                config=self.config,
                metric="_metric",
                mode="max",
                num_samples=8,
                reuse_actors=False,
            )
        self.assertCorrectExperimentOutput(out)

    def testOptunaWithStorage(self):
        from optuna.samplers import RandomSampler
        from optuna.storages import JournalStorage
        from optuna.storages.journal import JournalFileBackend

        from ray.tune.search.optuna import OptunaSearch

        np.random.seed(1000)  # At least one nan, inf, -inf and float
        storage_file_path = "/tmp/my_test_study.log"

        with self.check_searcher_checkpoint_errors_scope():
            out = tune.run(
                _invalid_objective,
                search_alg=OptunaSearch(
                    sampler=RandomSampler(seed=1234),
                    study_name="my_test_study",
                    storage=JournalStorage(
                        JournalFileBackend(file_path=storage_file_path)
                    ),
                ),
                config=self.config,
                metric="_metric",
                mode="max",
                num_samples=8,
                reuse_actors=False,
            )
        self.assertCorrectExperimentOutput(out)
        self.assertTrue(os.path.exists(storage_file_path))

    def testOptunaReportTooOften(self):
        from optuna.samplers import RandomSampler

        from ray.tune.search.optuna import OptunaSearch

        searcher = OptunaSearch(
            sampler=RandomSampler(seed=1234),
            space=OptunaSearch.convert_search_space(self.config),
            metric="metric",
            mode="max",
        )
        searcher.suggest("trial_1")
        searcher.on_trial_result("trial_1", {"training_iteration": 1, "metric": 1})
        searcher.on_trial_complete("trial_1", {"training_iteration": 2, "metric": 1})

        # Report after complete should not fail
        searcher.on_trial_result("trial_1", {"training_iteration": 3, "metric": 1})

        searcher.on_trial_complete("trial_1", {"training_iteration": 4, "metric": 1})

    def testZOOpt(self):
        self.skipTest(
            "Recent ZOOpt versions fail handling invalid values gracefully. "
            "Skipping until a fix is added in a future ZOOpt release."
        )
        from ray.tune.search.zoopt import ZOOptSearch

        # This seed tests that a nan result doesn't cause an error if it shows
        # up after the initial data collection phase.
        np.random.seed(1002)  # At least one nan, inf, -inf and float

        with self.check_searcher_checkpoint_errors_scope():
            out = tune.run(
                _invalid_objective,
                search_alg=ZOOptSearch(budget=25, parallel_num=4),
                config=self.config,
                metric="_metric",
                mode="max",
                num_samples=16,
                reuse_actors=False,
            )
        self.assertCorrectExperimentOutput(out)


class AddEvaluatedPointTest(unittest.TestCase):
    """
    Test add_evaluated_point method in searchers that support it.
    """

    def setUp(self):
        self.param_name = "report"
        self.valid_value = 1.0
        self.space = {self.param_name: tune.uniform(0.0, 5.0)}

        self.analysis = tune.run(
            _dummy_objective,
            config=self.space,
            metric="metric",
            num_samples=4,
            verbose=0,
        )

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4, num_gpus=0, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def run_add_evaluated_point(self, point, searcher, get_len_X, get_len_y):
        searcher = deepcopy(searcher)
        len_X = get_len_X(searcher)
        len_y = get_len_y(searcher)
        self.assertEqual(len_X, 0)
        self.assertEqual(len_y, 0)

        searcher.add_evaluated_point(point, 1.0)

        len_X = get_len_X(searcher)
        len_y = get_len_y(searcher)
        self.assertEqual(len_X, 1)
        self.assertEqual(len_y, 1)

        searcher.suggest("1")

    def run_add_evaluated_trials(self, searcher, get_len_X, get_len_y):
        searcher_copy = deepcopy(searcher)
        searcher_copy.add_evaluated_trials(self.analysis, "metric")
        self.assertEqual(get_len_X(searcher_copy), 4)
        self.assertEqual(get_len_y(searcher_copy), 4)
        searcher_copy.suggest("1")

        searcher_copy = deepcopy(searcher)
        searcher_copy.add_evaluated_trials(self.analysis.trials, "metric")
        self.assertEqual(get_len_X(searcher_copy), 4)
        self.assertEqual(get_len_y(searcher_copy), 4)
        searcher_copy.suggest("1")

        searcher_copy = deepcopy(searcher)
        searcher_copy.add_evaluated_trials(self.analysis.trials[0], "metric")
        self.assertEqual(get_len_X(searcher_copy), 1)
        self.assertEqual(get_len_y(searcher_copy), 1)
        searcher_copy.suggest("1")

    def testOptuna(self):
        from optuna.storages import JournalStorage
        from optuna.storages.journal import JournalFileBackend
        from optuna.trial import TrialState

        from ray.tune.search.optuna import OptunaSearch

        # OptunaSearch with in-memory storage
        searcher = OptunaSearch(
            space=self.space,
            storage=None,
            metric="metric",
            mode="max",
            points_to_evaluate=[{self.param_name: self.valid_value}],
            evaluated_rewards=[1.0],
        )

        get_len = lambda s: len(s._ot_study.trials)  # noqa E731

        self.assertGreater(get_len(searcher), 0)

        # OptunaSearch with external storage
        storage_file_path = "/tmp/my_test_study.log"
        searcher = OptunaSearch(
            space=self.space,
            study_name="my_test_study",
            storage=JournalStorage(JournalFileBackend(file_path=storage_file_path)),
            metric="metric",
            mode="max",
            points_to_evaluate=[{self.param_name: self.valid_value}],
            evaluated_rewards=[1.0],
        )

        get_len = lambda s: len(s._ot_study.trials)  # noqa E731

        self.assertGreater(get_len(searcher), 0)
        self.assertTrue(os.path.exists(storage_file_path))

        searcher = OptunaSearch(
            space=self.space,
            metric="metric",
            mode="max",
        )

        point = {
            self.param_name: self.valid_value,
        }

        self.assertEqual(get_len(searcher), 0)

        searcher.add_evaluated_point(point, 1.0, intermediate_values=[0.8, 0.9])
        self.assertEqual(get_len(searcher), 1)
        self.assertTrue(searcher._ot_study.trials[-1].state == TrialState.COMPLETE)

        searcher.add_evaluated_point(
            point, 1.0, intermediate_values=[0.8, 0.9], error=True
        )
        self.assertEqual(get_len(searcher), 2)
        self.assertTrue(searcher._ot_study.trials[-1].state == TrialState.FAIL)

        searcher.add_evaluated_point(
            point, 1.0, intermediate_values=[0.8, 0.9], pruned=True
        )
        self.assertEqual(get_len(searcher), 3)
        self.assertTrue(searcher._ot_study.trials[-1].state == TrialState.PRUNED)

        searcher.suggest("1")

        searcher = OptunaSearch(
            space=self.space,
            metric="metric",
            mode="max",
        )

        self.run_add_evaluated_trials(searcher, get_len, get_len)

        def dbr_space(trial):
            return {self.param_name: trial.suggest_float(self.param_name, 0.0, 5.0)}

        dbr_searcher = OptunaSearch(
            space=dbr_space,
            metric="metric",
            mode="max",
        )
        with self.assertRaises(TypeError):
            dbr_searcher.add_evaluated_point(point, 1.0)

    @pytest.mark.skipif(
        sys.version_info >= (3, 12), reason="HEBO doesn't support py312"
    )
    def testHEBO(self):
        if Version(pandas.__version__) >= Version("2.0.0"):
            pytest.skip("HEBO does not support pandas>=2.0.0")

        from ray.tune.search.hebo import HEBOSearch

        searcher = HEBOSearch(
            space=self.space,
            metric="metric",
            mode="max",
        )

        point = {
            self.param_name: self.valid_value,
        }

        get_len_X = lambda s: len(s._opt.X)  # noqa E731
        get_len_y = lambda s: len(s._opt.y)  # noqa E731

        self.run_add_evaluated_point(point, searcher, get_len_X, get_len_y)
        self.run_add_evaluated_trials(searcher, get_len_X, get_len_y)


class SaveRestoreCheckpointTest(unittest.TestCase):
    """
    Test searcher save and restore functionality.
    """

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.checkpoint_path = os.path.join(self.tempdir, "checkpoint.pkl")
        self.metric_name = "metric"
        self.config = {"a": tune.uniform(0.0, 5.0)}

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4, num_gpus=0, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def _on_trial_callbacks(self, searcher, trial_id):
        result = {
            TRAINING_ITERATION: 1,
            self.metric_name: 1,
            "config/a": 1.0,
            "time_total_s": 1,
        }
        searcher.on_trial_result(trial_id, result)
        searcher.on_trial_complete(trial_id, result)

    def _save(self, searcher):
        searcher.set_search_properties(
            metric=self.metric_name, mode="max", config=self.config
        )

        searcher.suggest("1")
        searcher.suggest("2")
        searcher.suggest("not_completed")
        self._on_trial_callbacks(searcher, "1")

        searcher.save(self.checkpoint_path)

    def _restore(self, searcher):
        # Restoration shouldn't require another call to `searcher.set_search_properties`
        searcher.restore(self.checkpoint_path)

        self._on_trial_callbacks(searcher, "2")
        searcher.suggest("3")
        self._on_trial_callbacks(searcher, "3")

        # NOTE: Trial "not_completed" that was suggested before saving never completes
        # We expect that it should still be tracked in the searcher state,
        # which is usually done in the searcher's `_live_trial_mapping`.
        # See individual searcher tests below for the special cases (e.g. Optuna, BOHB).
        if hasattr(searcher, "_live_trial_mapping"):
            assert "not_completed" in searcher._live_trial_mapping

    def testAx(self):
        from ax.service.ax_client import AxClient

        from ray.tune.search.ax import AxSearch

        converted_config = AxSearch.convert_search_space(self.config)
        client = AxClient()
        client.create_experiment(
            parameters=converted_config, objective_name=self.metric_name, minimize=False
        )
        searcher = AxSearch(ax_client=client)

        self._save(searcher)

        client = AxClient()
        client.create_experiment(
            parameters=converted_config, objective_name=self.metric_name, minimize=False
        )
        searcher = AxSearch(ax_client=client)
        self._restore(searcher)

    def testBayesOpt(self):
        from ray.tune.search.bayesopt import BayesOptSearch

        searcher = BayesOptSearch(
            space=self.config, metric=self.metric_name, mode="max"
        )
        self._save(searcher)

        searcher = BayesOptSearch()
        self._restore(searcher)

    @pytest.mark.skipif(
        sys.version_info >= (3, 12),
        reason="BOHB not yet supported for python 3.12+",
    )
    def testBOHB(self):
        from ray.tune.search.bohb import TuneBOHB

        searcher = TuneBOHB(space=self.config, metric=self.metric_name, mode="max")

        self._save(searcher)

        searcher = TuneBOHB()
        self._restore(searcher)

        assert "not_completed" in searcher.trial_to_params

    @pytest.mark.skipif(
        sys.version_info >= (3, 12), reason="HEBO doesn't support py312"
    )
    def testHEBO(self):
        if Version(pandas.__version__) >= Version("2.0.0"):
            pytest.skip("HEBO does not support pandas>=2.0.0")

        from ray.tune.search.hebo import HEBOSearch

        searcher = HEBOSearch(
            space=self.config,
            metric=self.metric_name,
            mode="max",
            random_state_seed=1234,
        )

        self._save(searcher)

        searcher = HEBOSearch()
        self._restore(searcher)

    def testHyperopt(self):
        from ray.tune.search.hyperopt import HyperOptSearch

        searcher = HyperOptSearch(
            space=self.config,
            metric=self.metric_name,
            mode="max",
        )
        self._save(searcher)

        searcher = HyperOptSearch()
        self._restore(searcher)

    def testNevergrad(self):
        import nevergrad as ng

        from ray.tune.search.nevergrad import NevergradSearch

        searcher = NevergradSearch(
            space=self.config,
            metric=self.metric_name,
            mode="max",
            optimizer=ng.optimizers.RandomSearch,
        )
        self._save(searcher)

        # `optimizer` is the only required argument
        searcher = NevergradSearch(optimizer=ng.optimizers.RandomSearch)
        self._restore(searcher)

    def testOptuna(self):
        from ray.tune.search.optuna import OptunaSearch

        searcher = OptunaSearch(
            space=self.config,
            storage=None,
            metric=self.metric_name,
            mode="max",
        )
        self._save(searcher)

        searcher = OptunaSearch()
        self._restore(searcher)

        assert "not_completed" in searcher._ot_trials

    def testOptunaWithStorage(self):
        from optuna.storages import JournalStorage
        from optuna.storages.journal import JournalFileBackend

        from ray.tune.search.optuna import OptunaSearch

        storage_file_path = "/tmp/my_test_study.log"
        searcher = OptunaSearch(
            space=self.config,
            study_name="my_test_study",
            storage=JournalStorage(JournalFileBackend(file_path=storage_file_path)),
            metric=self.metric_name,
            mode="max",
        )
        self._save(searcher)

        searcher = OptunaSearch()
        self._restore(searcher)

        assert "not_completed" in searcher._ot_trials
        self.assertTrue(os.path.exists(storage_file_path))

    def testZOOpt(self):
        from ray.tune.search.zoopt import ZOOptSearch

        searcher = ZOOptSearch(
            space=self.config,
            metric=self.metric_name,
            mode="max",
            budget=100,
            parallel_num=4,
        )

        self._save(searcher)

        # `budget` is the only required argument - will get replaced on restore
        searcher = ZOOptSearch(budget=0)
        self._restore(searcher)
        assert searcher._budget == 100


class MultiObjectiveTest(unittest.TestCase):
    """
    Test multi-objective optimization in searchers that support it.
    """

    def setUp(self):
        self.config = {
            "a": tune.uniform(0, 1),
            "b": tune.uniform(0, 1),
            "c": tune.uniform(0, 1),
        }

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4, num_gpus=0, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def testOptuna(self):
        from optuna.samplers import RandomSampler

        from ray.tune.search.optuna import OptunaSearch

        np.random.seed(1000)

        out = tune.run(
            _multi_objective,
            search_alg=OptunaSearch(
                sampler=RandomSampler(seed=1234),
                storage=None,
                metric=["a", "b", "c"],
                mode=["max", "min", "max"],
            ),
            config=self.config,
            num_samples=16,
            reuse_actors=False,
        )

        best_trial_a = out.get_best_trial("a", "max")
        self.assertGreaterEqual(best_trial_a.config["a"], 0.8)
        best_trial_b = out.get_best_trial("b", "min")
        self.assertGreaterEqual(best_trial_b.config["b"], 0.8)
        best_trial_c = out.get_best_trial("c", "max")
        self.assertGreaterEqual(best_trial_c.config["c"], 0.8)

    def testOptunaWithStorage(self):
        from optuna.samplers import RandomSampler
        from optuna.storages import JournalStorage
        from optuna.storages.journal import JournalFileBackend

        from ray.tune.search.optuna import OptunaSearch

        np.random.seed(1000)
        storage_file_path = "/tmp/my_test_study.log"

        out = tune.run(
            _multi_objective,
            search_alg=OptunaSearch(
                sampler=RandomSampler(seed=1234),
                study_name="my_test_study",
                storage=JournalStorage(JournalFileBackend(file_path=storage_file_path)),
                metric=["a", "b", "c"],
                mode=["max", "min", "max"],
            ),
            config=self.config,
            num_samples=16,
            reuse_actors=False,
        )

        best_trial_a = out.get_best_trial("a", "max")
        self.assertGreaterEqual(best_trial_a.config["a"], 0.8)
        best_trial_b = out.get_best_trial("b", "min")
        self.assertGreaterEqual(best_trial_b.config["b"], 0.8)
        best_trial_c = out.get_best_trial("c", "max")
        self.assertGreaterEqual(best_trial_c.config["c"], 0.8)
        self.assertTrue(os.path.exists(storage_file_path))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
