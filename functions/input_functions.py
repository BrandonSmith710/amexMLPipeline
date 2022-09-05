from psycopg2 import sql


def yes_no_input() -> str:
    # prompt user for input until yes or no is specified

    answer = ''
    while not answer:
        answer = input('Yes/No: ')
        if 'no' in answer.lower():
            return False
        elif 'yes' in answer.lower():
            return True
        answer = ''
        print('Entry must be yes or no')


def table_input() -> str:
    # prompt user for input until a table is specified

    table = ''
    while not table:
        table = input(
            'Enter table customers or customers_train: '
            ).lower()
        if not table in ['customers', 'customers_train']:
            table = ''
            print('Table does not exist')
    return table


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


def validate_cid(curs, table, id) -> str or bool:
    '''verify the id exists in table's primary key index
    parameters: curs - psycopg2 cursor
                table - customers or customers_train
                id - uppercase alphanumeric cid
    returns: cid if valid, else False
    '''

    try:
        curs.execute(
            sql.SQL("SELECT name FROM {table} WHERE cid = %s;").format(
                table = sql.Identifier(table)), (id,))
        name = curs.fetchall()[0]
        return name
    except:
        return False


def field_inputs(adding_field, table) -> list or None:
    '''take and validate user input for fields to update
       parameters: adding_field - bool
                   table - customers or customers_train
       returns: list of tuples ex. [(field, new_value)],
                or None if user chooses to abandon update
    '''

    updates = []
    fields = {
        'cid': cid_check, 'name': name_check, 'age': int_check,
        'gender': gender_check, 'owns_car': owner_check, 'owns_house':
        owner_check, 'num_children': int_check, 'yearly_income': float,
        'num_days_employed': int_check, 'occupation': occupation_check,
        'num_family_members': int_check, 'migrant_worker': int_check,
        'yearly_payments': float, 'credit_limit': float,
        'percent_credit_limit_used': int_check, 'credit_score': int_check,
        'prev_defaults': int_check, 'default_in_last_six_months': int_check,
        }
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
                    print('Incompatible value entry', e)
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