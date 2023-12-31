#import os,sys

#pwd = os.path.dirname(os.path.realpath(__file__))
#sys.path.append(pwd+"/../../")
#sys.path.append(pwd+"/../") 
#os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'AnlsTool.settings')
#
#
#import django
#django.setup()

from django.db.models import Q

from NormalTask.models import ParentTask, Version, Task, ExecFunction, Input, InputFileSheet, InputFileColumn, OutputSheet, OutputColumn, SqlCode, JobHistory, DeveQueue, QueueChangeRecord

get_model ={"ParentTask":ParentTask,"Task":Task}

def list2db(sorted_item, model_name):
    DeveQueue.objects.filter(model_name=model_name).delete()
    for index, item in enumerate(sorted_item):
        dq = DeveQueue()
        if index == 0:
            dq.is_root = True
        else:
            dq.is_root = False
        dq.model_name = model_name
        dq.model_id = item.id
        if len(sorted_item)== index+1:
            dq.next_id = -1
        else:
            dq.next_id = sorted_item[index+1].id
        dq.save()

def get_modify(model_name):
    res = []
    rec_list = QueueChangeRecord.objects.filter(model_name=model_name).order_by('change_time')
    # modify_list = DeveQueue.objects.filter(if_modify=True).filter(model_name=model_name)
    modify_list = DeveQueue.objects.filter(model_name=model_name)
    l_list = [i.model_id for i in modify_list]
    for rec in rec_list: 
        if rec.model_id in l_list:
            res.append({rec.prev_id:rec.model_id})
    return res
    
def del_modify(model_name):
    modify_list = DeveQueue.objects.filter(if_modify=True).filter(model_name=model_name)
    for item in modify_list:
        item.if_modify = False
        item.save()
    

def do_modify(modify_rec,model_name):
    for d in modify_rec:
        for prev in d.keys():
            if prev == -1:
                root_item = DeveQueue.objects.filter(model_name=model_name).get(is_root=True)
                if root_item.model_id == d[prev]:
                    this_item = DeveQueue.objects.filter(model_name=model_name).get(model_id = d[prev])
                    this_item.if_modify = True
                    this_item.save()
                else:
                    raw_prev = DeveQueue.objects.filter(model_name=model_name).get(next_id = d[prev])
                    this_item = DeveQueue.objects.filter(model_name=model_name).get(model_id = d[prev]) 
                    raw_prev.next_id = this_item.next_id
                    raw_prev.save()
                    root_item = DeveQueue.objects.filter(model_name=model_name).get(is_root=True)
                    root_item.is_root = False
                    root_item.save()
                    this_item.is_root = True
                    this_item.next_id = root_item.model_id
                    this_item.if_modify = True
                    this_item.save()
            else:
                raw_prev = DeveQueue.objects.filter(model_name=model_name).get(next_id = d[prev])
                this_item = DeveQueue.objects.filter(model_name=model_name).get(model_id = d[prev])
                prev_item = DeveQueue.objects.filter(model_name=model_name).get(model_id = prev)
                raw_prev.next_id = this_item.next_id
                raw_prev.save()
                this_item.next_id = prev_item.next_id
                this_item.if_modify = True
                this_item.save()
                prev_item.next_id = this_item.model_id
                prev_item.save()


def initqueue(model_name ='ParentTask',order_by='create_time'):
    queue_model = get_model[model_name]
    all_item = queue_model.objects.all() 
    if len(all_item)>0:
        if 'level' in dir(all_item[0]):
            imp_item = all_item.filter(level=1)
            norm_item = all_item.filter(level=2)
        elif 'priority' in dir(all_item[0]):
            lianhui_imp_item = all_item.filter(priority=0)
            imp_item = all_item.filter(priority=1)
            norm_item = all_item.filter(priority=2)
        sorted_list = sorted(list(lianhui_imp_item),key=lambda x:x.__getattribute__(order_by))
        sorted_list.extend(sorted(list(imp_item),key=lambda x:x.__getattribute__(order_by)))
        sorted_list.extend(sorted(list(norm_item),key=lambda x:x.__getattribute__(order_by)))
        try:
            modify_rec = get_modify(model_name)
        except Exception as e:
            print('initqueue error:'+str(e))
            modify_rec = {}
        list2db(sorted_list, model_name)
        do_modify(modify_rec, model_name)
    else:
        DeveQueue.objects.filter(model_name=model_name).delete()


def queue2list(model_name = 'ParentTask', order_by='create_time'):
    res = []
    queue_list = DeveQueue.objects.filter(model_name=model_name)
    queue_set = set()
    if len(queue_list)==0:
        return res
    this_item = queue_list.get(is_root=True)
    while True:
        if this_item.model_id not in queue_set:
            if model_name == 'ParentTask':
                task = ParentTask.objects.get(id=this_item.model_id)
                if task.status!='finished' and task.status!='cancel' and task.status!='check' and task.status!='failed'  and task.is_valid==True:  
                    res.append(task)
            elif model_name == 'Task':
                task = Task.objects.get(id=this_item.model_id)
                if task.status!='finished' and task.status!='cancel' and task.if_valid==True: 
                    res.append(task)
            queue_set.add(this_item.model_id)
        else:
            break
        if this_item.next_id != -1:
            this_item = queue_list.get(model_id=this_item.next_id)
        else:
            break
    return res

if __name__ =="__main__":
    initqueue('ParentTask')

