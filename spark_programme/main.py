from Insertion import *
from Adjustment import *
from Querying import *




def main():
    task_parameter = input("\033[;36m"+'''Put the operation to perform:
    1.Insertion:
    2.Adjustment:
    3.Querying:
    '''+"\033[;37m")
    if task_parameter == '1' or task_parameter == 1:
        zone = input("Put where the data is going to be inserted (ej>raw,ej>cur,ej>ref): ")
        delt = input("""Put whether there are deltas or not (ej>*(todas),ej>no,ej>nombre,dni)
        and consider that if the partitions is the first one, it should not contain deltas: """)
        insertion1 = Insertion(zone,delt)
        if zone == 'raw' and delt == 'no':
            insertion1.raw_without_deltas()
        if zone == 'raw' and delt != 'no':
            insertion1.raw_with_deltas()
        if zone == 'cur' and delt == 'no':
            insertion1.cur_without_deltas()
        if zone == 'cur' and delt != 'no':
            insertion1.cur_with_deltas()

    if task_parameter == '2' or task_parameter == 2:
        adjustment1 = Adjustment()
        adjustment1.adjust_header()


    if task_parameter == '3' or task_parameter == 3:
        querying1 = Querying()
        task = input("""Put what to do:
        a.Querying and getting fast result:
        b.Querying and saving it to a file in a specified path: """)
        if task == 'a':
            querying1.fast_visualization()
        if task == 'b':
            querying1.saving()



main()