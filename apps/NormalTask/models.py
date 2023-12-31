from django.db import models
from django.db.models import DateField, DateTimeField, sql,Max,Q
from django.contrib.auth import get_user_model
from datetime import datetime

# 引用自定义User模型
# User = get_user_model()
AUTH_USER_MODEL = 'Auth.User'

# 任务列表
class Task(models.Model):
    """
    子任务
    每个子任务都对应一个可以执行应用 由版本表与父任务关联
    """
    class Meta:
        db_table = 'normaltask_sontask'
        verbose_name = "子任务"
        verbose_name_plural = verbose_name

    LEVEL = (
        (1,'特级'),
        (2,'一般'),
    )
    TaskStatus =(
        ('confirming', '待确认'),
        ('ranking', '排队中'),
        ('developing', '开发中'),
        # ('checking', '复核中'),  # 对外数据协查在开发后需复核
        # ('testing', '测试中'),  # 功能需求使用
        ('finished', '已完成'),
        ('repairing', 'BUG修复中'),
    )
    TaskReusltTypes = (
        ('file','输出结果文件'),
        ('module','输出查询功能'),
    )
    title = models.CharField(verbose_name='标题', max_length=100, )
    level = models.IntegerField(verbose_name='级别', choices = LEVEL, default=2)
    type = models.CharField(verbose_name='任务类型', max_length=100, choices=TaskReusltTypes)
    status = models.CharField(verbose_name='任务状态', max_length=50, choices=TaskStatus, default='confirming')
    desc = models.TextField(verbose_name='任务描述')
    deadline = models.DateField(verbose_name='截止时间',blank=True,null=True)
    
    creater = models.ForeignKey(AUTH_USER_MODEL, verbose_name='创建人员',  related_name='task_create', on_delete=models.SET_DEFAULT, default = 58)
    create_time = models.DateTimeField('创建时间', default=datetime.now)
    
    developer = models.ForeignKey(AUTH_USER_MODEL, verbose_name='开发人员', related_name='task_develop', on_delete=models.SET_DEFAULT, default='')
    assign_time = models.DateTimeField(verbose_name='指派时间',null=True,blank=True)
    
    finish_by = models.ForeignKey(verbose_name='由谁完成',to=AUTH_USER_MODEL,on_delete=models.CASCADE,null=True,blank=True,related_name='task_finish_by')
    finish_time = models.DateTimeField(verbose_name='完成时间',null=True,blank=True)
    consumed_hours = models.IntegerField(verbose_name='耗时(小时)',null=True,blank=True)
    
    update_time = models.DateTimeField(verbose_name='更新时间',auto_now=True)
    
    if_valid = models.BooleanField(verbose_name='有效标志(1:有效,0:无效)')
    if_model = models.BooleanField(verbose_name='是否为模块', default=False)

    def __str__(self):
        return self.title

class File(models.Model):
    FileTypes = (
        ('req_file', '需求附件'),
        ('req_reason_file', '需求依据附件'),
        ('bug_file', '问题反馈附件'),
        ('res_file','结果附件'),
    )

    class Meta:
        db_table = 'anlstool_file'

    type = models.CharField(verbose_name='附件类型', max_length=100, choices=FileTypes)
    file_name = models.CharField(verbose_name='附件名称',max_length=100)
    file_path = models.CharField(verbose_name='附件路径',max_length=255)
    upload_time = models.DateTimeField(verbose_name='上传时间',auto_created=True)
    is_valid = models.BooleanField(verbose_name='是否有效',default=True)

    def __str__(self):
        return '%s-%s' % (self.type,self.file_name)

    # 逻辑删除
    def logic_delete(self):
        self.is_valid = False
        self.save()
        
class File2Task(models.Model):
    task = models.ForeignKey(Task, verbose_name='子任务', on_delete=models.CASCADE)
    file = models.ForeignKey(File, verbose_name='结果文件', on_delete=models.CASCADE)
    
    def __str__(self):
        return str(self.task)+'-'+str(self.file)
        
    class Meta:
        db_table = 'normaltask_file2task'
        verbose_name = "结果文件表"
        verbose_name_plural = verbose_name
        
        
class ParentTask(models.Model):
    RequirmentTypes = (
        ('query', (
                ('research', 'research'),
                ('routine', 'routine'),
            )
        ),
        ('file', (
                ('inquery', 'inquery'),
                ('measure', 'measure'),
            )
        ),
    )
    RequirmentPriorities = (
        (1, '特急'),
        (2, '一般'),
    )
    RequirmentStatus = (
        ('wait','待技术人员评估'),
        ('conforming','待业务人员确认'),
        ('conformed','已确认'),
        ('developing','开发中'),
        ('accepting','验收中'),            # 计算需求使用
        ('finished','已完成'),
        ('cancel','已取消'),
        ('check','审核中'),
        ('failed','审核不通过')
    )
    
        # 最新任务编号
    def get_last_seq(self):
        last_seq = ParentTask.objects.aggregate(Max('seq')).get('seq__max')
        if not last_seq:
            last_seq = 0
        return last_seq
    
    # 不同版本的同一个需求需求编号一致，新增需求时该字段+1，修改任务时该字段保持不变。前端显示但不可编辑
    seq = models.IntegerField(verbose_name='需求编号')
    title = models.CharField(verbose_name='标题', max_length=100 )   
    type = models.CharField(verbose_name='需求类型', max_length=100, choices=RequirmentTypes)
    priority = models.IntegerField(verbose_name='优先级',choices=RequirmentPriorities,default=2)
    desc = models.TextField(verbose_name='需求描述')
    requirment_files = models.ManyToManyField(verbose_name='需求附件',to='File',related_name='req_files',related_query_name='req_file',blank=True)
    reason = models.TextField(verbose_name='需求依据')
    reason_file = models.ManyToManyField(verbose_name='需求依据附件',to='File',related_name='reason_files',related_query_name='reason_file',blank=True)
    deadline = models.DateField(verbose_name='截止时间',blank=True,null=True)
    status = models.CharField(verbose_name='需求状态',max_length=50,choices=RequirmentStatus,blank=True)
    is_valid = models.BooleanField(verbose_name='是否有效',default=True)
    is_model = models.BooleanField(verbose_name='是否为专项模块',default=False)
    mod_url = models.CharField(verbose_name='专项模块路由', max_length=100, default='')   

    create_by = models.ForeignKey(verbose_name='由谁创建',to=AUTH_USER_MODEL,on_delete=models.CASCADE,null=True,blank=True,related_name='req_create_by')
    create_time = models.DateTimeField(verbose_name='创建时间',auto_now_add=True)
    update_time = models.DateTimeField(verbose_name='更新时间',auto_now=True)
    confirm_by = models.ForeignKey(verbose_name='由谁确认',to=AUTH_USER_MODEL,on_delete=models.CASCADE,null=True,blank=True,related_name='req_confirm_by')
    confirm_time = models.DateTimeField(verbose_name='确认时间',null=True,blank=True)
    closed_by = models.ForeignKey(verbose_name='由谁关闭',to=AUTH_USER_MODEL,on_delete=models.CASCADE,null=True,blank=True,related_name='req_closed_by')
    closed_time = models.DateTimeField(verbose_name='关闭时间',null=True,blank=True)
    canceled_by = models.ForeignKey(verbose_name='由谁取消',to=AUTH_USER_MODEL,on_delete=models.CASCADE,null=True,blank=True,related_name='req_canceled_by')
    canceled_time = models.DateTimeField(verbose_name='取消时间',null=True,blank=True)
    
    class Meta:
        db_table = 'normaltask_parenttask'
        verbose_name = "父任务"
        verbose_name_plural = verbose_name
    
    def __str__(self):
        return str(self.title)        
    
    
class Version(models.Model):
    """
    版本表
    父任务不直接与子任务关联 而与版本表 产生关联 
    """
    version_id = models.IntegerField(verbose_name='子任务版本编号')
    version_num = models.IntegerField(verbose_name='版本号')
    parent_task = models.ForeignKey(ParentTask, verbose_name='需求', on_delete=models.PROTECT)
    demand_seq = models.IntegerField(verbose_name='需求编号')
    son_task = models.ForeignKey(Task, verbose_name='子任务', on_delete=models.CASCADE)
    if_exec = models.BooleanField(verbose_name='是否可执行',default=True)
    class Meta:
        db_table = 'normaltask_version'
        verbose_name = "版本"
        verbose_name_plural = verbose_name
    
    def __str__(self):
        return str(self.son_task)+'版本:'+str(self.version_num)

        
class ExecFunction(models.Model):
    """
    解析器配置
    """
    DB_TYPE = (
        (0,'无'),
        (1,'postgre主环境'),
        (2,'postgre备环境'),
        (3,'OracleP01'),
        (4,'Oracle仿真'),
        (5,'Oracle万得'),
        (6,'其他'),
    )
    exec_id = models.IntegerField('执行器编号', help_text='执行器编号')
    name = models.CharField('执行器中文名', max_length=30, help_text='执行器中文名')
    func_name = models.CharField('执行器函数名', max_length=30, help_text='执行器函数名')
    db_type = models.IntegerField('执行器所连数据库',choices=DB_TYPE,default=0, help_text='执行器所连数据库')
    class Meta:
        db_table = 'normaltask_execfunction'
        verbose_name = "解析器配置"
        verbose_name_plural = verbose_name
        
    def __str__(self):
        return self.name
    
class Input(models.Model):
    """
    子任务输入
    """
    DATA_TYPE = (
        ('Date','日期'),
        ('DateTime','时刻'),
        ('String','字符'),
        ('Number','数值'),
        ('File','文件'),
        ('List','列表'),
    )
    task = models.ForeignKey(Task, on_delete=models.CASCADE, verbose_name = '子任务')
    name = models.CharField('输入字段名称', default='', max_length=100, help_text='输入字段名称')
    type = models.CharField('输入字段类型', choices=DATA_TYPE, max_length=10,help_text='输入字段类型')
    input_id = models.IntegerField('输入参数顺序', default=0, help_text='输入参数顺序')
    detail = models.TextField('输入字段描述', default='')
    default_value = models.CharField('输入字段默认值', default='', max_length=30, help_text='输入字段默认值',null=True, blank=True)
    replace_key = models.CharField('替换sql字段名', default='', max_length=100, help_text='替换sql字段名')
    class Meta:
        db_table = 'normaltask_input'
        verbose_name = "输入参数"
        verbose_name_plural = verbose_name
        
    def __str__(self):
        return self.name
        
class InputFileSheet(models.Model):
    """
    文件输入Sheet
    """
    input = models.ForeignKey(Input, on_delete=models.CASCADE, verbose_name = '文件输入字段')
    exec_id = models.ForeignKey(ExecFunction, on_delete=models.PROTECT, verbose_name='临时表写入所用数据库')
    name = models.CharField('表格sheet对应的elk临时表表名', default='', max_length=100, help_text='表格sheet对应的elk临时表表名')
    sheet_id = models.IntegerField('上传表格sheet页编号',help_text='上传表格sheet页编号')
    class Meta:
        db_table = 'normaltask_inputfilesheet'
        verbose_name = "文件输入Sheet"
        verbose_name_plural = verbose_name
        
    def __str__(self):
        return self.name
        
class InputFileColumn(models.Model):
    """
    文件输入字段
    """
    sheet = models.ForeignKey(InputFileSheet, on_delete=models.CASCADE, verbose_name = '文件输入Sheet页')
    name = models.CharField('字段名', default='', max_length=30, help_text='表格列名，同时也是插入elk表格中的列名')
    type = models.CharField('字段类型', default='', max_length=30, help_text='写入数据库的字段类型')
    class Meta:
        db_table = 'normaltask_inputfilecolumn'
        verbose_name = "文件输入字段"
        verbose_name_plural = verbose_name
        
    def __str__(self):
        return self.name

class OutputSheet(models.Model):
    """
    输出Sheet页名称
    """
    task = models.ForeignKey(Task, on_delete=models.CASCADE, verbose_name = '子任务')
    sheet_output_id = models.IntegerField('输出表格编号',help_text = '输出表格编号')
    name = models.CharField('输出表格名称', default = '', max_length=100, help_text='输出表格名称')
    detail =  models.TextField('输出表格算法描述', default='', help_text='输出表格算法描述',null=True, blank=True)
    class Meta:
        db_table = 'normaltask_outputsheet'
        verbose_name = "输出Sheet页名称"
        verbose_name_plural = verbose_name
        
    def __str__(self):
        return self.task.title +' : ' +self.name
        
class OutputColumn(models.Model):
    """
    输出字段
    """
    sheet = models.ForeignKey(OutputSheet, on_delete=models.CASCADE, verbose_name = '输出Sheet页')
    name = models.CharField('输出字段名称', default = '', max_length=100, help_text='输出字段名称')
    detail = models.TextField('输出字段描述', default='')
    replace_key = models.CharField('输出字段替换字段',default='', max_length=100, help_text='输出字段替换字段')
    class Meta:
        db_table = 'normaltask_outputcolumn'
        verbose_name = "输出字段"
        verbose_name_plural = verbose_name    

    def __str__(self):
        return self.name
        
class SqlCode(models.Model):
    """
    sql代码
    """
    FILE_TYPE = (
        (0,'不替换参数'),
        (1,'参数替换')
    )
    DISPLAY_TYPE = (
        (0,'展示'),
        (1,'不展示'),
    )
    REPLACE_STYLE =(
        (0,'%(name)s'),
        (1,'#{name}'),
        (2,'&name'),
    )
    task = models.ForeignKey(Task, on_delete=models.CASCADE, verbose_name='子任务')
    exec_id = models.ForeignKey(ExecFunction, on_delete=models.PROTECT, verbose_name='解析所用数据库')
    sql_id = models.IntegerField('执行顺序',help_text='执行顺序')
    code = models.TextField('SQL代码', default='', help_text='SQL代码')
    file_type = models.IntegerField('代码类型', choices=FILE_TYPE, help_text='代码类型')
    display = models.IntegerField('是否展示结果', choices=DISPLAY_TYPE, help_text='是否展示结果')
    replace_style = models.IntegerField('占位符替换风格', choices=REPLACE_STYLE,default=0 ,help_text='占位符替换风格')
    if_mulit = models.BooleanField(verbose_name='是否为跨库导数据sql(t:跨库，f:不跨库)', default=False,null=True, blank=True)
    from_conn =  models.IntegerField('数据来源数据库id',help_text='数据来源数据库id',null=True, blank=True)
    to_conn = models.IntegerField('数据导入数据库id',help_text='数据导入数据库id',null=True, blank=True)
    table_name = models.CharField('数据导入表名', default = '', max_length=100, help_text='数据导入表名',null=True, blank=True)
    if_create = models.BooleanField(verbose_name='是否建表(t:建表，f:不建表)', default=False,null=True, blank=True)
    if_temp = models.BooleanField(verbose_name='是否建为临时表(t:建临时表，f:不建临时表)', default=False,null=True, blank=True)
    class Meta:
        db_table = 'normaltask_sqlcode'
        verbose_name = "sql代码"
        verbose_name_plural = verbose_name  
    
    def __str__(self):
        return self.task.title+' SQL: '+str(self.sql_id)

class JobHistory(models.Model):
    """
    常规查询执行历史记录
    """
    EXEC_STATUS = (
        (0,'查询中'),
        (1,'执行成功'),
        (2,'sql执行失败'),
        (3,'python执行失败'),
        (4,'临时表插入失败')
    )
    task = models.ForeignKey(Task, on_delete=models.CASCADE, verbose_name='子任务')
    create_time = models.DateTimeField('创建时间', default=datetime.now)
    renew_time = models.DateTimeField('更新时间', default=datetime.now)
    user = models.ForeignKey(AUTH_USER_MODEL, verbose_name='查询用户', on_delete=models.SET_DEFAULT, default = '')
    input_json = models.TextField('输入参数json', default='{}', help_text='输入参数json')
    input_file = models.TextField('查询上传文件', default='[]', help_text='查询上传文件')
    res_file = models.TextField('结果文件', default='[]', help_text='结果文件')
    exec_status = models.SmallIntegerField('执行结果', choices=EXEC_STATUS, default=0, help_text='执行结果')
    exec_info = models.TextField('python报错信息', default='', help_text='python报错信息')
    sql_done = models.SmallIntegerField('sql完成数量', default=0, help_text='sql完成数量')
    sql_all = models.SmallIntegerField('sql总数', default=0, help_text='sql总数')
    pid = models.IntegerField('进程号', help_text='进程号', default=0)
    class Meta:
        db_table = 'normaltask_jobhistory'
        verbose_name = "常规查询执行历史记录"
        verbose_name_plural = verbose_name 
    def __str__(self):
        return self.task.title+' 创建日期:'+str(self.create_time)+' 用户: '+str(self.user.full_name)
        
class ChangeHistory(models.Model):
    """
    变动历史（暂时只有需求变动，其他的尚未添加）
    """
    class Meta:
        db_table = 'normaltask_demandhistory'

    user = models.ForeignKey(verbose_name='用户', to=AUTH_USER_MODEL, on_delete=models.CASCADE)
    model_name =  models.CharField(verbose_name='模型',max_length=50)
    change_id = models.IntegerField(verbose_name='变动记录id')
    field_name = models.CharField(verbose_name='字段', max_length=50)
    old = models.TextField(verbose_name='旧值' ,null=True, blank=True)
    new = models.TextField(verbose_name='新值', null=True, blank=True)
    change_time = models.DateTimeField('修改时间', default=datetime.now)
    remark = models.TextField(verbose_name='备注',blank=True,null=True)


class DeveQueue(models.Model):
    """
    队列（暂时只有需求，开发队列尚未完成）
    """
    class Meta:
        db_table = 'normaltask_queue'

    model_name =  models.CharField(verbose_name='模型',max_length=50)
    model_id = models.IntegerField(verbose_name='模型id')
    next_id = models.IntegerField(verbose_name='模型id')
    is_root = models.BooleanField(verbose_name='是否队首(t:队首，f:非队首)', default=False)
    if_modify = models.BooleanField(verbose_name='是否调整位置', default=False)


class QueueChangeRecord(models.Model):
    """
    手动变更队列的记录
    """
    class Meta:
        db_table = 'normaltask_queuechg'

    change_time = models.DateTimeField('修改时间', default=datetime.now)
    model_name =  models.CharField(verbose_name='模型',max_length=50)
    model_id = models.IntegerField(verbose_name='模型id')
    prev_id = models.IntegerField(verbose_name='前序id')
    change_reason = models.TextField(verbose_name='修改原因',blank=True,null=True)

