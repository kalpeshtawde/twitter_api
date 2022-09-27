import os

from django.http import HttpResponse
from django.template import loader
from django.core.management import call_command
from django.http import Http404


file_path = "/code/output.xlsx"

def twitter(request):
    tweet_id = request.GET.get('tweet_id', None)
    user_id = request.GET.get('user_id', None)
    start_date = request.GET.get('start_date', None)
    end_date = request.GET.get('end_date', None)

    context = {
        'tweet_id': tweet_id,
        'user_id': user_id,
    }

    if tweet_id:
        try:
            call_command('tweet_stat', '--tweet_id', tweet_id)
        except Exception as e:
            print(f"Error: {e}")
            context["error"] = f"Error: {e}"
    elif user_id:
        call_command('tweet_stat', '--user_id', user_id, '--start_date',
                 start_date, '--end_date', end_date)

    if not os.path.exists(file_path):
        context['file_exists'] = 'False'
    else:
        context['file_exists'] = 'True'

    template = loader.get_template('app/homepage.html')
    return HttpResponse(template.render(context, request))


def download_result(request):

    if os.path.exists(file_path):
        with open(file_path, 'rb') as fh:
            response = HttpResponse(
                fh.read(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            response['Content-Disposition'] = 'attachment; filename=output.xlsx'
            return response
    else:
        raise Http404
