import yaml
import glob
import re

def _escape_sql_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")

def check_examples():
    example_files = glob.glob("examples/*.yaml")
    for file_path in example_files:
        print(f"Checking {file_path}...")
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
            examples = data.get('examples', [])
            for i, ex in enumerate(examples):
                question = ex.get('question', '')
                sql = ex.get('sql', '')
                
                # Check question
                escaped_q = _escape_sql_string(question)
                # If we wrap this in '', is it valid?
                # Simple check: does it end in an unescaped backslash?
                # In our escape function, all \ become \\.
                # So the only way it fails is if the logic of wrapping is broken.
                
                # Let's try to find if any string has a weird number of quotes or ends strangely
                if question.count("'") % 2 != 0:
                    # Not necessarily an error if escaped correctly, but worth noting
                    pass
                
                # Another possibility: non-printable characters or weird unicode
                if any(ord(c) < 32 and c not in '\n\r\t' for c in question):
                    print(f"  [!] Weird character in question at index {i}")

                # The error was "Unclosed string literal at [5:82]"
                # This usually means a quote was opened but not closed.
                # In the batch script:
                # f"STRUCT('{question}' AS question, "
                # If question is: "What's the price?"
                # It becomes: "STRUCT('What\'s the price?' AS question, "
                # This looks fine.
                
                # What if the question contains a newline that isn't handled?
                if '\n' in question:
                    print(f"  [!] Newline in question at index {i}: {question!r}")
                if '\n' in sql:
                    # SQL usually has newlines, but we escape them? 
                    # No, _escape_sql_string doesn't escape newlines.
                    # BigQuery supports multi-line strings if quoted with '' or "", 
                    # but maybe not in a STRUCT literal? Actually it should.
                    pass

check_examples()
