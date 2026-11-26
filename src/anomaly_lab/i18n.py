"""All user-facing text lives here: one table per language, same keys in both.

Language order: --lang  >  PYSPARK_ANOMALY_LANG  >  LC_ALL / LC_MESSAGES / LANG  >  English.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

LANGS = ("en", "ko")
ENV_VAR = "PYSPARK_ANOMALY_LANG"

MESSAGES: dict[str, dict[str, str]] = {
    "en": {
        # --- help ---
        "help_description": "PySpark practice demo 5/5: find anomalies in sensor numbers, grade them with planted answers, pick alert thresholds. Uses practice (synthetic) data.",
        "help_step": "step to run: generate, baseline, residual, cluster, evaluate, alert, or run-all (everything)",
        "help_devices": "number of devices; the last one is a constant reference sensor (default 6)",
        "help_days": "days of hourly data per device (default 14, minimum 3)",
        "help_seed": "random seed; same seed gives the same data (default 42)",
        "help_out": "folder for results (default out)",
        "help_lang": "output language: en or ko",
        "help_threshold": "robust z threshold for an alarm (default 3.5)",
        "help_zthreshold": "moving z-score threshold for an alarm (default 3.0)",
        "help_kmeans_top": "share of rows with the largest KMeans distance that raise an alarm (default 0.03)",
        "help_n": "m-of-n rule: look at the last n hours (default 3)",
        "help_m": "m-of-n rule: alert if at least m of those hours are anomalous (default 2)",
        "help_budget": "alerts per day you are willing to look at (default 1.5)",
        # --- errors ---
        "err_java_missing": "Java was not found. Spark 4 needs Java 17 or newer.\n  macOS:  brew install openjdk@21\n  Ubuntu: apt install openjdk-21-jre-headless (administrator rights needed)\n  Then set JAVA_HOME (or put java on PATH) and run again.",
        "err_java_old": "Java {found} was found, but Spark 4 needs Java 17 or newer.\n  Install Java 17+ (for example: brew install openjdk@21) and point JAVA_HOME at it.",
        "err_no_input": "Missing input: {path} was not found.\n  Run `pyspark-anomaly {step}` first (or `run-all`), or check --out.",
        "help_help": "show this help message and exit",
        "help_usage_prefix": "usage: ",
        "help_positionals": "positional arguments",
        "help_options": "options",
        "err_arg": "Invalid arguments (a wrong step name, an unknown option, or a value of the wrong type).\n  Run `pyspark-anomaly --help` to see the options.",
        "err_arg_choice": "  Problem: {name} does not accept '{value}'.",
        "err_arg_value": "  Problem: invalid value for {name}: {value}",
        "err_arg_missing_value": "  Problem: {name} needs a value.",
        "err_arg_unknown": "  Problem: unknown option or extra argument: {value}",
        "err_arg_required": "  Problem: missing required argument: {name}",
        "err_arg_other": "  Problem: {value}",
        "err_devices_min": "Invalid option: --devices must be >= 2",
        "err_days_min": "Invalid option: --days must be >= 3",
        "err_kmeans_top_range": "Invalid option: --kmeans-top must be between 0 and 1",
        "err_m_range": "Invalid option: --m must be between 1 and --n",
        "err_budget_min": "Invalid option: --budget must be greater than 0",
        # --- common ---
        "synthetic_notice": "Practice data: synthetic sensor readings made by code (not real machines). Planted answers are used only for grading.",
        "meaning_title": "What this means",
        "col_method": "method",
        "col_alerts": "alerts",
        "col_recall": "recall",
        "col_fpr": "fpr",
        "col_precision": "precision",
        "col_undecidable": "undecidable",
        "col_threshold": "threshold",
        "col_per_day": "alerts/day",
        "col_delay": "delay(h)",
        "col_rule": "rule",
        "col_point": "point recall",
        "col_sustained": "drift recall",
        "col_combo": "combo recall",
        "col_type": "type",
        "col_rows": "rows",
        # --- generate ---
        "generate_title": "Step 1 generate: {rows} rows, {devices} devices x {days} days (seed {seed})",
        "generate_meaning": "The data has {injected} planted anomalous rows (point, drift, combo). Device {ref} is a reference sensor with a constant value: any score that divides by its spread is undecidable there. The planted answers (is_injected) are only used to grade, never to score.",
        # --- baseline ---
        "baseline_title": "Step 2 baseline: raw-value detectors",
        "baseline_meaning": "raw_iqr raised {iqr} alarms and moving_z caught {z} planted anomalous rows: the daily temperature cycle widens both baselines, so real anomalies hide inside the normal swing. {und} rows were undecidable for moving_z (warm-up hours and the constant reference sensor): try_divide gave NULL instead of crashing the job.",
        # --- residual ---
        "residual_title": "Step 3 residual: remove the usual value of each device and hour",
        "residual_meaning": "After subtracting the usual value of each hour, robust_z finds {rz} of the point anomalies while raw_iqr finds {iqr}. Define 'normal' per group and per hour before scoring.",
        # --- cluster ---
        "cluster_title": "Step 4 cluster: KMeans distance on scaled temperature and vibration residuals",
        "cluster_row_total": "combo (planted)",
        "cluster_row_missed": "combo missed by robust_z",
        "cluster_row_caught": "  ... caught by kmeans",
        "cluster_meaning": "{combo} planted combo anomalies exist; robust_z missed {missed} of them, and the KMeans distance caught {caught} of those {missed}. Each value looks fine alone, but the pair breaks the usual relation.",
        # --- evaluate ---
        "evaluate_title": "Step 5 evaluate: every method graded with the planted answers",
        "evaluate_curve_title": "robust_z threshold curve (alerts per day vs recall)",
        "evaluate_verdict_both": "No method wins everywhere: robust_z is precise but blind to combos, kmeans sees combos but also raises more false alarms.",
        "evaluate_verdict_combo": "On this run kmeans sees combos ({combo} of them) and its precision ({kp}) is not lower than robust_z ({rp}), so its extra false alarms do not show here, but robust_z still misses the combos.",
        "evaluate_verdict_alarms": "On this run kmeans raises more false alarms (precision {kp} vs robust_z {rp}) and finds only {combo} of the combos, so it does not reliably see them.",
        "evaluate_verdict_neither": "On this run kmeans does not clearly beat robust_z: its precision ({kp}) is not lower than robust_z ({rp}), but it finds only {combo} of the combos.",
        "evaluate_meaning": "{verdict} At threshold {top} robust_z still finds only {rec} of the point anomalies, because weak ones sit inside normal noise. Decide how many alerts per day you can read first, then pick the threshold (next step).",
        "evaluate_saved": "Curve saved to {path}",
        # --- alert ---
        "alert_title": "Step 6 alert: budget {budget} alerts/day, {m}-of-{n} rule",
        "alert_budget_line": "Lowest threshold within budget: {thr} ({per_day} alerts/day).",
        "alert_budget_none": "No threshold fits the budget; showing the highest one ({thr}). Raise --budget or look at fewer devices.",
        "alert_label_plain": "no rule",
        "alert_label_rule": "{m}-of-{n} rule",
        "alert_label_flood": "no rule, rule's threshold",
        "alert_meaning": "With a budget of {budget} alerts/day: no rule needs threshold {t_plain} and finds {s_plain} of the drifts. The {m}-of-{n} rule allows threshold {t_rule} and finds {s_rule} of the drifts, but it hides one-hour blips (point recall {p_rule}). The same low threshold without the rule would raise {flood} alerts/day. Use the rule for slow drifts, not for sudden spikes.",
        "alert_meaning_over": "Even the highest threshold ({t_plain}) raises {per_day} alerts/day, more than your budget of {budget}. No threshold fits, so the numbers above show the least bad choice. Raise the budget, look at fewer devices, or accept more alerts.",
        "alert_saved": "Alerts saved to {path} (one folder per device)",
        # --- run-all ---
        "run_all_done": "Done. Results are in {out}",
    },
    "ko": {
        "help_description": "PySpark 연습 데모 5/5: 센서 수치에서 이상치를 찾고, 심어 둔 정답으로 성능을 재고, 알림 임계값을 정합니다. 연습용(합성) 데이터를 씁니다.",
        "help_step": "실행할 단계: generate, baseline, residual, cluster, evaluate, alert, run-all(전부)",
        "help_devices": "장비 수. 마지막 장비는 값이 일정한 기준 센서입니다 (기본 6)",
        "help_days": "장비마다 만들 시간별 데이터의 일수 (기본 14, 최소 3)",
        "help_seed": "난수 시드. 같은 시드면 같은 데이터가 나옵니다 (기본 42)",
        "help_out": "결과를 저장할 폴더 (기본 out)",
        "help_lang": "출력 언어: en 또는 ko",
        "help_threshold": "알림으로 보는 robust z 임계값 (기본 3.5)",
        "help_zthreshold": "알림으로 보는 이동 z-점수 임계값 (기본 3.0)",
        "help_kmeans_top": "KMeans 거리가 가장 큰 상위 몇 퍼센트를 알림으로 볼지 비율 (기본 0.03)",
        "help_n": "n중 m 규칙: 최근 n시간을 본다 (기본 3)",
        "help_m": "n중 m 규칙: 그중 m시간 이상 이상이면 알림 (기본 2)",
        "help_budget": "하루에 볼 수 있는 알림 수 (기본 1.5)",
        "err_java_missing": "Java를 찾지 못했습니다. Spark 4는 Java 17 이상이 필요합니다.\n  macOS:  brew install openjdk@21\n  Ubuntu: apt install openjdk-21-jre-headless (관리자 권한 필요)\n  설치 후 JAVA_HOME을 지정하거나 java를 PATH에 넣고 다시 실행하세요.",
        "err_java_old": "Java {found} 버전이 발견됐지만 Spark 4는 Java 17 이상이 필요합니다.\n  Java 17 이상을 설치하고(예: brew install openjdk@21) JAVA_HOME을 그쪽으로 지정하세요.",
        "err_no_input": "입력이 없습니다: {path} 을(를) 찾지 못했습니다.\n  먼저 `pyspark-anomaly {step}` (또는 `run-all`)을 실행하거나 --out 을 확인하세요.",
        "help_help": "이 도움말을 보여 주고 종료합니다",
        "help_usage_prefix": "사용법: ",
        "help_positionals": "위치 인자",
        "help_options": "옵션",
        "err_arg": "인자가 올바르지 않습니다(단계 이름 오류, 모르는 옵션, 값의 형식 오류 등). 사용 가능한 옵션은 `pyspark-anomaly --help` 로 확인하세요.",
        "err_arg_choice": "  문제: {name} 에는 '{value}' 를 쓸 수 없습니다.",
        "err_arg_value": "  문제: {name} 의 값이 올바르지 않습니다: {value}",
        "err_arg_missing_value": "  문제: {name} 에 값이 필요합니다.",
        "err_arg_unknown": "  문제: 모르는 옵션이거나 남는 인자입니다: {value}",
        "err_arg_required": "  문제: 필요한 인자가 빠졌습니다: {name}",
        "err_arg_other": "  문제: {value}",
        "err_devices_min": "옵션이 올바르지 않습니다: --devices 는 2 이상이어야 합니다",
        "err_days_min": "옵션이 올바르지 않습니다: --days 는 3 이상이어야 합니다",
        "err_kmeans_top_range": "옵션이 올바르지 않습니다: --kmeans-top 은 0보다 크고 1보다 작아야 합니다",
        "err_m_range": "옵션이 올바르지 않습니다: --m 은 1 이상이고 --n 이하여야 합니다",
        "err_budget_min": "옵션이 올바르지 않습니다: --budget 은 0보다 커야 합니다",
        "synthetic_notice": "연습용 데이터: 코드로 만든 합성 센서 값입니다(실제 장비 기록 아님). 심어 둔 정답은 채점에만 씁니다.",
        "meaning_title": "그래서 무슨 뜻일까요",
        "col_method": "방법",
        "col_alerts": "알림 수",
        "col_recall": "탐지율(recall)",
        "col_fpr": "오탐률(fpr)",
        "col_precision": "정밀도(precision)",
        "col_undecidable": "판단 불가",
        "col_threshold": "임계값",
        "col_per_day": "하루 알림",
        "col_delay": "지연(시간)",
        "col_rule": "규칙",
        "col_point": "점 이상 탐지율",
        "col_sustained": "지속 이상 탐지율",
        "col_combo": "조합 이상 탐지율",
        "col_type": "종류",
        "col_rows": "행 수",
        "generate_title": "1단계 generate: {rows}행, 장비 {devices}대 x {days}일 (시드 {seed})",
        "generate_meaning": "심어 둔 이상 행은 모두 {injected}개(점·지속·조합)입니다. 장비 {ref} 는 값이 일정한 기준 센서라, 퍼짐 정도로 나누는 점수는 여기서 판단 불가가 됩니다. 심은 정답(is_injected)은 채점에만 쓰고 점수 계산에는 쓰지 않습니다.",
        "baseline_title": "2단계 baseline: 원값 기준 탐지기",
        "baseline_meaning": "raw_iqr 는 알림 {iqr}건, moving_z 는 심은 이상 행 {z}개를 잡았습니다. 하루 주기의 온도 변화가 두 기준선을 모두 넓혀서 진짜 이상이 평소 흔들림 속에 묻힙니다. moving_z 의 판단 불가 {und}행(첫 시간들과 기준 센서)은 try_divide 가 작업을 멈추지 않고 NULL 을 돌려준 결과입니다.",
        "residual_title": "3단계 residual: 장비·시간대별 평소 값을 빼기",
        "residual_meaning": "시간대별 평소 값을 뺀 뒤 robust_z 는 점 이상의 {rz} 를 잡지만 raw_iqr 는 {iqr} 입니다. 점수를 매기기 전에 '평소'를 그룹별·시간대별로 정해야 합니다.",
        "cluster_title": "4단계 cluster: 스케일한 온도·진동 잔차의 KMeans 거리",
        "cluster_row_total": "조합 이상(심은 것)",
        "cluster_row_missed": "robust_z 가 놓친 조합 이상",
        "cluster_row_caught": "  ... kmeans 가 잡음",
        "cluster_meaning": "조합 이상 {combo}개 중 robust_z 가 놓친 것은 {missed}개이고, 그 {missed}개 중 KMeans 거리가 {caught}개를 잡았습니다. 값 하나씩은 멀쩡해도 둘의 조합이 평소 관계를 깨면 여러 지표를 함께 봐야 보입니다.",
        "evaluate_title": "5단계 evaluate: 심은 정답으로 방법별 채점",
        "evaluate_curve_title": "robust_z 임계값 곡선 (하루 알림 수와 탐지율)",
        "evaluate_verdict_both": "모든 면에서 이기는 방법은 없습니다. robust_z 는 정밀하지만 조합 이상을 못 보고, kmeans 는 조합 이상을 보지만 오탐도 더 냅니다.",
        "evaluate_verdict_combo": "이번 실행에서 kmeans 는 조합 이상을 {combo} 잡고 정밀도({kp})도 robust_z({rp})보다 낮지 않아 오탐이 더 많다는 점은 드러나지 않지만, robust_z 는 여전히 조합 이상을 놓칩니다.",
        "evaluate_verdict_alarms": "이번 실행에서 kmeans 는 오탐이 더 많고(정밀도 {kp}, robust_z {rp}) 조합 이상은 {combo} 만 잡아 조합을 믿고 볼 수는 없습니다.",
        "evaluate_verdict_neither": "이번 실행에서 kmeans 가 robust_z 보다 뚜렷이 낫지는 않습니다. 정밀도({kp})는 robust_z({rp})보다 낮지 않지만 조합 이상은 {combo} 만 잡습니다.",
        "evaluate_meaning": "{verdict} 임계값 {top} 에서도 robust_z 는 점 이상의 {rec} 만 잡습니다. 약한 이상치는 평소 잡음 안에 있기 때문입니다. 하루에 읽을 수 있는 알림 수를 먼저 정하고 그에 맞춰 임계값을 고르세요(다음 단계).",
        "evaluate_saved": "곡선 저장: {path}",
        "alert_title": "6단계 alert: 하루 알림 예산 {budget}건, {n}중 {m} 규칙",
        "alert_budget_line": "예산 안에서 가장 낮은 임계값: {thr} (하루 알림 {per_day}건).",
        "alert_budget_none": "예산에 맞는 임계값이 없어 가장 높은 값({thr})을 보여 줍니다. --budget 을 올리거나 장비 수를 줄여 보세요.",
        "alert_label_plain": "규칙 없음",
        "alert_label_rule": "{n}중 {m} 규칙",
        "alert_label_flood": "규칙 없음, 규칙의 임계값",
        "alert_meaning": "하루 {budget}건 예산에서 규칙이 없으면 임계값 {t_plain} 이 필요하고 지속 이상의 {s_plain} 를 잡습니다. {n}중 {m} 규칙은 임계값 {t_rule} 까지 낮출 수 있어 지속 이상의 {s_rule} 를 잡지만, 한 시간짜리 튐은 숨깁니다(점 이상 탐지율 {p_rule}). 같은 낮은 임계값을 규칙 없이 쓰면 하루 {flood}건이 울립니다. 이 규칙은 서서히 번지는 이상에 쓰고, 갑작스러운 튐에는 쓰지 마세요.",
        "alert_meaning_over": "가장 높은 임계값({t_plain})에서도 하루 {per_day}건이 울려 예산 {budget}건을 넘습니다. 예산에 맞는 임계값이 없어 위 숫자는 그나마 나은 선택일 뿐입니다. 예산을 올리거나 장비 수를 줄이거나 더 많은 알림을 감수해야 합니다.",
        "alert_saved": "알림 저장: {path} (장비별 폴더)",
        "run_all_done": "완료. 결과는 {out} 에 있습니다",
    },
}


def resolve_lang(cli_lang: str | None = None, environ: Mapping[str, str] | None = None) -> str:
    """Pick the language: --lang, then PYSPARK_ANOMALY_LANG, then locale variables, else en."""
    environ = os.environ if environ is None else environ
    if cli_lang in LANGS:
        return cli_lang
    own = (environ.get(ENV_VAR) or "").lower()
    if own in LANGS:
        return own
    for name in ("LC_ALL", "LC_MESSAGES", "LANG"):
        value = environ.get(name)
        if value:  # the first variable that is set decides
            return "ko" if value.lower().startswith("ko") else "en"
    return "en"


def tr(lang: str, key: str, **values: object) -> str:
    """Look up a message and fill in its {placeholders}."""
    text = MESSAGES[lang][key]
    return text.format(**values) if values else text
