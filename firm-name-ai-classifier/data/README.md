# Data format

## Input

Your input CSV needs at least two columns:

| Column | Description |
|---|---|
| `name_aff` | The first company name (e.g., from your primary database) |
| `matched_name` | The second company name (e.g., a candidate match from another database) |

Any other columns in the file will be kept in the output.

Example:
```
name_aff,matched_name
TOYOTA MOTOR CORP,TOYOTA MOTORS
APPLE INC,APPLE COMPUTER
PETROBRAS SA,VALE SA
```

## Output

The script adds two columns to the input:

| Column | Values | Description |
|---|---|---|
| `Q1` | `Yes` or `Non` | Whether the model thinks they're the same company |
| `Q2` | `1` to `10` | Model's confidence in its Q1 answer |

Example output:
```
name_aff,matched_name,Q1,Q2
TOYOTA MOTOR CORP,TOYOTA MOTORS,Yes,9
APPLE INC,APPLE COMPUTER,Yes,8
PETROBRAS SA,VALE SA,Non,9
```

## Tips on interpreting results

- `Q1 = Yes, Q2 >= 8` → high confidence match, safe to use
- `Q1 = Yes, Q2 <= 5` → uncertain — worth reviewing manually
- `Q1 = Non, Q2 >= 8` → the model is confident these are different companies
- `Q1 = Non, Q2 <= 5` → borderline case, review manually if precision matters

For large datasets, a reasonable workflow is:
1. Accept all `Q1 = Yes, Q2 >= 7` automatically
2. Review `Q1 = Yes, Q2 < 7` manually (should be a small fraction)
3. Discard `Q1 = Non`
