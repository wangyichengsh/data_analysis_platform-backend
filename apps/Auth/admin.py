from django.contrib import admin
from django.contrib.auth import get_user_model
from django.utils.translation import gettext_lazy as _
from django.contrib.auth.admin import UserAdmin
from .models import UserGroup

# 引用自定义User模型
User = get_user_model()


class GroupInlineAdmin(admin.TabularInline):
    # model = User.groups.through
    model = UserGroup
    verbose_name = '用户组'
    extra = 1
    can_delete = True
    # template = 'edit_inline/inline_usergroup.html'

    # exclude = ['eff_date','end_date']

    def formfield_for_manytomany(self, db_field, request, **kwargs):
        # if db_field.name == "car":
        kwargs["queryset"] = User.objects.filter(username=request.user)
        print(kwargs)
        print(kwargs["queryset"])
        return super().formfield_for_foreignkey(db_field, request, **kwargs)


@admin.register(User)
class UserAdmin(UserAdmin):
    fieldsets = (
        (None, {'fields': ('username', 'password')}),
        (_('Personal info'), {'fields': ('full_name', 'email')}),
        (_('Permissions'), {
            'fields': ('is_active', 'is_staff', 'is_superuser',
                       # 'groups',
                       'user_permissions'),
        }),
        (_('Important dates'), {'fields': ('last_login', 'date_joined')}),
    )
    list_display = ('full_name', 'username', 'email',  'is_active','is_staff', 'is_superuser')
    search_fields = ('username', 'full_name')
    inlines = [
        GroupInlineAdmin,
    ]

    # # 覆盖get_form，在组变更时维护UserGroup
    # def get_form(self, request, obj=None, **kwargs):
    #     defaults = {}
    #     if obj is None:
    #         defaults['form'] = self.add_form
    #
    #     defaults.update(kwargs)
    #
    #     return super().get_form(request, obj, **defaults)
    #
    #
    #     # if request.method == 'POST':
    #     #     try:
    #     #         user_id = obj.id
    #     #         print(user_id)
    #     #         a = UserGroup.objects.filter(user_id=user_id, is_valid=False, end_date=datetime.date(3000,12,31))
    #     #         print(a.values())
    #     #         UserGroup.objects.filter(user_id=user_id, is_valid=False, end_date=datetime.date(3000,12,31)).update(is_valid=False, end_date=timezone.now().date())
    #     #         b = UserGroup.objects.filter(user_id=user_id, is_valid=False)
    #     #         print(b.values())
    #     #     except:
    #     #         pass

