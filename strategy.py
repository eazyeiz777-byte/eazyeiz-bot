# strategy.py
import random

def evaluate_symbol(df):
    if df is None or len(df) < 20:
        return False, None

    if random.random() < 0.05:
        return True, "🧪 Test signal: Random strategy match"

    return False, None
