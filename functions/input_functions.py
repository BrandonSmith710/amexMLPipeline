from psycopg2 import sql



def numeric_prompt():
    print('Filter operations are >, <, >=, <=, =')
    print('''If using more than one operator, separate each with a
          comma. ex. >= 2, <= 6''')

def string_prompt():
    print('Filter operations are start =, end =, contains =')
    print('''If using more than one operator, separate each with a
          comma. ex. start = Dan, end = el, contains = aniel''')


def yes_no_input() -> bool:
    # prompt user for yes/no input until yes or no is specified

    answer = ''
    while not answer:
        answer = input('Yes/No: ')
        if 'no' in answer.lower():
            return False
        elif 'yes' in answer.lower():
            return True
        answer = ''
        print('Entry must be yes or no')


def table_input() -> str or None:
    # prompt user for table name until a table is specified

    table = ''
    while not table:
        table = input(
            'Enter table customers or customers_train: '
            ).lower()
        if not table in ['customers', 'customers_train']:
            if 'exit' in table.lower():
                return None
            table = ''
            print('Table does not exist')
    return table

def add_or_delete() -> bool or str:
    # prompt user for instruction until add or delete is specified

    answer = ''
    while not answer:
        print('Choose to add or delete from a table')
        answer = input('Add/Delete: ').lower()
        if 'exit' in answer:
            return 'exit'
        if 'delete' in answer:
            return False
        if 'add' in answer:
            return True
        answer = ''
        print('Acceptable entries are: Add, add, Delete, delete')


def cid_check(val) -> str or Exception:
    # verify the supplied value matches the format of primary key cid

    val = val.split('_')
    if len(val) == 2:
        if val[0].isalpha() and len(val[0]) == 3:
            if val[1].isdigit() and len(val[1]) == 6:
                return '_'.join(val).upper()
    raise Exception


def name_check(val) -> str or Exception:
    # verify the supplied name is all alphanumeric characters

    if all(x.isalnum() for x in val.split()):
        return val.title()
    raise Exception


def gender_check(val) -> str or Exception:
    # verify the supplied value contains 'f' or 'm'

    if 'f' in val.lower():
        return 'F'
    elif 'm' in val.lower():
        return 'M'
    raise Exception


def owner_check(val) -> str or Exception:
    # verify the supplied value contains 'y' or 'n'

    if 'y' in val.lower():
        return 'Y'
    elif 'n' in val.lower():
        return 'N'
    raise Exception


def int_check(val) -> int or Exception:
    # verify the supplied value can be interpreted as int

    if val.isdigit():
        return int(val)
    raise Exception

def conjoin_filter(params) -> str:
    '''format a list parameters to SQL syntax
    parameters: params - list of SQL conditions
                ex. [age >= 35, age < 50, name ILIKE 'alex%']
    '''

    return ' AND '.join(params)


def process_filter_numeric(answer, entry) -> list or None:
    
    params = []
    ops = [o.strip() for o in entry.split(',')]
    for op in ops:
        if any(c in '<>=' for c in op):
            a = ''.join(filter(lambda x: x in '><=', op))
            b = ''.join(filter(lambda x: x.isdigit(), op))
            if b:
                params.append(a + ' ' + b)
            else:
                print(f'Invalid entry: {op}')
                return None
        else:
            print(f'Invalid entry: {op}')
            return None
    return [f'{answer} {param}' for param in params]


def process_filter_string(answer, entry) -> list or None:

    params = []
    ops = [o.strip() for o in entry.split(',')]
    for op in ops:
        if '=' in op:
            a, b = op.split('=')
            b = b.strip()
            if 'start' in a:
                params.append(b + '%')
            elif 'end' in a:
                params.append('%' + b)
            elif 'contains' in a:
                params.append('%' + b + '%')
            else:
                print(f'Invalid entry: {op}')
                return None
        else:
            print(f'Invalid entry: {op}')
            return None
    return [f"{answer} ILIKE '{param}'" for param in params]


def occupation_check(val) -> str or Exception:
    # verify the supplied value can be found in the jobs list
    
    jobs = ['Other', 'Laborers', 'Core staff', 'Accountants',
       'High skill tech staff', 'Sales staff', 'Managers', 'Drivers',
       'Medicine staff', 'Cleaning staff', 'HR staff', 'Security staff',
       'Cooking staff', 'Waiters/barmen staff', 'Low-skill Laborers',
       'Private service staff', 'Secretaries', 'Realty agents',
       'IT staff']
    for job in jobs:
        if val.lower() in job.lower():
            return job
        val_chars = ''.join(filter(lambda x: x.isalnum(), val)).lower()
        job_chars = ''.join(filter(lambda x: x.isalnum(), job)).lower()
        if val_chars in job_chars:
            return job
    raise Exception


validator = {
        'cid': cid_check, 'name': name_check, 'age': int_check,
        'gender': gender_check, 'owns_car': owner_check, 'owns_house':
        owner_check, 'num_children': int_check, 'yearly_income': float,
        'num_days_employed': int_check, 'occupation': occupation_check,
        'num_family_members': int_check, 'migrant_worker': int_check,
        'yearly_payments': float, 'credit_limit': float,
        'percent_credit_limit_used': int_check, 'credit_score': int_check,
        'prev_defaults': int_check, 'default_in_last_six_months': int_check,
        }

def collect_insert_input(table) -> list or None:
    '''prompt user for data corresponding to the fields in table
    parameters: table - customers or customers_train
    returns: list containing table data
            or None if user enters 'exit'
    '''

    fields = validator.copy()
    updates = []
    if table == 'customers_train':
        fields['credit_card_default'] = int_check
    for field in fields:
        answer = ''
        while not answer:
            answer = input(f'Enter {field}: ')
            if 'exit' in answer.lower():
                print('Exiting...')
                return None
            try:
                answer = fields[field](answer)
            except Exception as e:
                print('Incompatible value entry', e)
                answer = ''
            if answer == 0:
                updates.append(0)
                break
        else:
            if type(answer) == str:
                answer = f"'{answer}'"
            updates.append(answer)
    return updates


def collect_query_params(table):

    fields = validator.copy()
    filters = []
    adding = True
    if table == 'customers_train':
        fields['credit_card_default'] = int_check
    while adding:
        answer = input('Enter field: ').lower().strip()
        if 'exit' in answer:
            print('Exiting...')
            break
        if answer in fields:
            ready_to_filter = False
            checker = fields[answer]
            while not ready_to_filter:
                if checker in [int_check, float]:
                    prompt = numeric_prompt
                    processor = process_filter_numeric
                else:
                    prompt = string_prompt
                    processor = process_filter_string
                prompt()
                entry = input('Enter operations: ')
                if 'exit' in entry.lower():
                    adding = False
                    break
                processed = processor(answer = answer, entry = entry)
                if not processed:
                    continue
                params = conjoin_filter(params = processed)
                filters.append(params)
                ready_to_filter = True          
            print('Filter by another field?')
            adding = yes_no_input()
        else:
            print('Invalid field entry')
    return filters
    

def field_inputs(adding_field, table) -> list or None:
    '''take and validate user input for fields to update
       parameters: adding_field - bool
                   table - customers or customers_train
                   validator - dictionary containing functions to
                               validate input
       returns: list of tuples ex. [(field, new_value)],
                or None if user chooses to abandon update
    '''

    fields = validator.copy()
    updates = []
    if table == 'customers_train':
        fields['credit_card_default'] = int_check
    while adding_field:
        entry = input(
            'Enter field to update and its new value separated by comma: '
            )
        if 'exit' in entry.lower():
            if updates:
                print('Do you want to complete these updates?')
                print(updates)
                complete = yes_no_input()
                if complete:
                    return updates
            print("Exiting...")
            return None
        elif ',' in entry:
            entry = entry.split(',')
            field, value = entry[0].lower().strip(), entry[1].strip()
            if field in fields:
                try:
                    value = fields[field](value)
                except Exception as e:
                    print('Incompatible value entry')
                    continue
                print(f'Are you sure you want to set {field} = {value}?')
                proceed = yes_no_input()
                if proceed:
                    updates.append((field, value))
                print('Update another field?')
                adding_field = yes_no_input()
            else:
                print(f'Could not find field "{field}"')
        else:
            print('Invalid entry')
    return updates