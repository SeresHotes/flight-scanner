#!/usr/bin/env python3
"""CLI-обёртка. Логика перенесена в core/aggregate.py без смены поведения.

Модуль реэкспортирует функции core.aggregate для обратной совместимости
(исторически его импортировали как `import aggregate_flights as agg`).
"""
from core.aggregate import *  # noqa: F401,F403
from core.aggregate import main

if __name__ == "__main__":
    main()
