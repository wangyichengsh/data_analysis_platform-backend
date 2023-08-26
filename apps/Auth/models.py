from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager,AbstractUser,PermissionsMixin, Permission ,Group
from django.utils.translation import gettext_lazy as _
from django.contrib.auth.validators import UnicodeUsernameValidator
from django.core.mail import send_mail
from django.utils import timezone
import datetime
from django.dispatch import receiver
from django.db.models.signals import pre_save, post_save, post_delete,pre_delete

class ManyToManyValidField(models.ManyToManyField):
    pass



# 覆盖原有UserManager
class UserManager(BaseUserManager):
    use_in_migrations = True

    def _create_user(self, username, full_name, email, password, **extra_fields):
        """
        Create and save a user with the given username, email, and password.
        """
        if not username:
            raise ValueError('The given username must be set')
        email = self.normalize_email(email)
        username = self.model.normalize_username(username)
        user = self.model(username=username, full_name=full_name, email=email, **extra_fields)
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_user(self, username, full_name, email=None, password=None, **extra_fields):
        extra_fields.setdefault('is_staff', False)
        extra_fields.setdefault('is_superuser', False)
        # extra_fields.setdefault('is_valid', True)
        return self._create_user(username, full_name, email, password, **extra_fields)

    def create_superuser(self, username, full_name, email, password, **extra_fields):
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        # extra_fields.setdefault('is_valid', True)

        if extra_fields.get('is_staff') is not True:
            raise ValueError('Superuser must have is_staff=True.')
        if extra_fields.get('is_superuser') is not True:
            raise ValueError('Superuser must have is_superuser=True.')

        return self._create_user(username, full_name, email, password, **extra_fields)



# 覆盖Django原有User模型
class User(AbstractBaseUser, PermissionsMixin):
    class Meta:
        verbose_name = "用户"
        verbose_name_plural = '用户'
        db_table = 'auth_user'

    username_validator = UnicodeUsernameValidator()

    # 字段定义
    username = models.CharField(
        _('username'),
        max_length=150,
        unique=True,
        help_text=_('Required. 150 characters or fewer. Letters, digits and @/./+/-/_ only.'),
        validators=[username_validator],
        error_messages={
            'unique': _("A user with that username already exists."),
        },
    )
    full_name = models.CharField('人员姓名', max_length=20, blank=True)
    email = models.EmailField(_('email address'), blank=True)
    is_staff = models.BooleanField(
        _('管理站点权限'),
        default=False,
        help_text=_('Designates whether the user can log into this admin site.'),
    )
    is_active = models.BooleanField(
        _('active'),
        default=True,
        help_text=_(
            'Designates whether this user should be treated as active. '
            'Unselect this instead of deleting accounts.'
        ),
    )
    date_joined = models.DateTimeField(_('date joined'), default=timezone.now)
    fail_login = models.IntegerField(verbose_name='失败登录次数',default=0)

    groups = models.ManyToManyField(
        Group,
        verbose_name=_('groups'),
        blank=True,
        help_text=_(
            'The groups this user belongs to. A user will get all permissions '
            'granted to each of their groups.'
        ),
        related_name = "user_set",
        related_query_name = "user",
        through = "UserGroup",
    )

    objects = UserManager()

    EMAIL_FIELD = 'email'
    USERNAME_FIELD = 'username'
    REQUIRED_FIELDS = ['full_name','email']

    def __str__(self):
        return '%s(%s)' % (self.full_name,self.username)

    def clean(self):
        super().clean()
        self.email = self.__class__.objects.normalize_email(self.email)

    def get_full_name(self):
        """
        Return the first_name plus the last_name, with a space in between.
        """
        full_name = '%s' % (self.full_name)
        return full_name.strip()

    def get_short_name(self):
        """Return the short name for the user."""
        return self.full_name

    def email_user(self, subject, message, from_email=None, **kwargs):
        """Send an email to this user."""
        send_mail(subject, message, from_email, [self.email], **kwargs)

    def get_group_id(self):
        group_ids = []
        groups = list(self.usergroup_set.filter(is_valid = True,group_type = 1).values('group').distinct())
        if len(groups) > 0:
            for d in groups:
                group_ids.append(d['group'])

        # Group.objects.filter(id__in=UserGroup.objects.filter(user__groups__user_set == self.objects.all() , is_valid = True).get('group'))
        return group_ids

    def get_group_name(self):
        group_ids = self.get_group_id()
        group_names = []
        if len(group_ids) > 0:
            groups = list(Group.objects.filter(id__in = group_ids).values('name'))
            if len(groups) > 0:
                for d in groups:
                    group_names.append(d['name'])
        return group_names


# 增加用户与组之间关系的历史变动模型，需要在用户组变更处增加修改该表数据的代码，用于审计(应放入审计app中)
class UserGroup(models.Model):
    class Meta:
        db_table = 'auth_user_group'

    user = models.ForeignKey(verbose_name='用户id', to=User, on_delete=models.CASCADE)
    group = models.ForeignKey(verbose_name='组id', to=Group, on_delete=models.CASCADE)
    is_teamleader = models.BooleanField(verbose_name='是否组长',default=False)
    # is_factgroup = models.BooleanField(verbose_name='实际属于该组',default=True)
    group_type = models.IntegerField(verbose_name='所属组类型',default=1,choices=((1,'实际所属组'),(2,'拥有本组访问权限')))
    eff_date = models.DateField(verbose_name='生效日期',null=True)#,auto_now_add=True)
    end_date = models.DateField(verbose_name='结束日期',null=True,default=datetime.date(3000,12,31))
    is_valid = models.BooleanField(verbose_name='是否有效',default=True)


@receiver(post_save, sender=UserGroup)
def post_save_UserGroup(sender, instance, **kwargs):
    UserGroup.objects.filter(is_valid=False, end_date=datetime.date(3000, 12, 31)).update(end_date=timezone.now().date())
    UserGroup.objects.filter(is_valid=True, end_date__lt=datetime.date(3000, 12, 31)).update(end_date=datetime.date(3000, 12, 31))
    UserGroup.objects.filter(is_valid=True, eff_date__isnull=True).update(eff_date=timezone.now().date())

if __name__ == '__main__':
    user = User.objects.get(id = 1)
