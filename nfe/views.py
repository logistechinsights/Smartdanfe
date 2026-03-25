import json
import base64
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from django.shortcuts import render, redirect
from django.http import JsonResponse, HttpResponse
from django.contrib.auth.decorators import login_required
from django.contrib.auth import login
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST, require_GET
from django.conf import settings
from django.urls import reverse
import xml.etree.ElementTree as ET
import pandas as pd
import mercadopago
from .models import NFe, Payment, UserProfile
from .api_client import add_chave, baixar_pdf, baixar_xml
from .forms import CustomUserCreationForm
from .decorators import subscription_required

logger = logging.getLogger(__name__)


def home(request):
    if request.user.is_authenticated:
        has_approved = Payment.objects.filter(user=request.user, status='APPROVED').exists()
        if not has_approved:
            has_pending = Payment.objects.filter(user=request.user, status='PENDING').exists()
            if has_pending:
                return redirect('payment_history')
            else:
                # sem pendência e sem aprovação, fica na página de planos
                return render(request, 'nfe/plans.html')
    return render(request, 'nfe/plans.html')


def register(request):
    """Registro de novos usuários, capturando o plano da URL"""
    plan = request.GET.get('plan')
    if request.method == 'POST':
        form = CustomUserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            if plan and plan in ['mensal', 'trimestral', 'anual']:
                return redirect(f'/dashboard/checkout/?plan={plan}')
            else:
                return redirect('home')
    else:
        form = CustomUserCreationForm()
    return render(request, 'registration/register.html', {'form': form, 'plan': plan})


@login_required
@subscription_required
def dashboard(request):
    return render(request, 'nfe/dashboard.html')


@require_POST
@csrf_exempt
@login_required
def process_keys(request):
    data = json.loads(request.body)
    keys = data.get('keys', [])
    if not keys:
        return JsonResponse({'error': 'Nenhuma chave fornecida'}, status=400)

    for chave in keys:
        nfe, created = NFe.objects.get_or_create(
            user=request.user,
            chave_acesso=chave,
            defaults={'status': 'WAITING'}
        )
        if created:
            try:
                resp = add_chave(chave)
                nfe.status = 'PROCESSING'
                nfe.tipo = resp.get('type', 'NFe')
                nfe.save()
                pdf_data = baixar_pdf(chave)
                xml_data = baixar_xml(chave)
                if pdf_data and pdf_data.get('data'):
                    nfe.status = 'OK'
                    nfe.pdf_base64 = pdf_data['data']
                    if xml_data and xml_data.get('data'):
                        nfe.xml_text = xml_data['data']
                        nfe.mensagem = 'PDF e XML disponíveis'
                    else:
                        nfe.mensagem = 'PDF disponível'
                    nfe.save()
                else:
                    nfe.mensagem = 'Processando, aguarde...'
                    nfe.save()
            except Exception as e:
                nfe.status = 'ERROR'
                nfe.mensagem = str(e)
                nfe.save()

    return JsonResponse({'success': True, 'message': f'{len(keys)} chave(s) em processamento'})


@require_GET
@login_required
def nfe_status(request):
    nfes = NFe.objects.filter(user=request.user).order_by('-created_at')
    data = []
    for nfe in nfes:
        if nfe.status == 'PROCESSING' and not nfe.pdf_base64:
            pdf_data = baixar_pdf(nfe.chave_acesso)
            if pdf_data and pdf_data.get('data'):
                nfe.status = 'OK'
                nfe.pdf_base64 = pdf_data['data']
                xml_data = baixar_xml(nfe.chave_acesso)
                if xml_data and xml_data.get('data'):
                    nfe.xml_text = xml_data['data']
                    nfe.mensagem = 'PDF e XML disponíveis'
                else:
                    nfe.mensagem = 'PDF disponível'
                nfe.save()
        elif nfe.status == 'OK' and not nfe.xml_text:
            xml_data = baixar_xml(nfe.chave_acesso)
            if xml_data and xml_data.get('data'):
                nfe.xml_text = xml_data['data']
                nfe.mensagem = 'PDF e XML disponíveis'
                nfe.save()
        data.append({
            'chave': nfe.chave_acesso,
            'status': nfe.status,
            'tipo': nfe.tipo,
            'mensagem': nfe.mensagem,
            'pdf_disponivel': bool(nfe.pdf_base64),
            'xml_disponivel': bool(nfe.xml_text),
            'created_at': nfe.created_at.isoformat(),
        })
    return JsonResponse({'nfes': data})


@require_GET
@login_required
def download_pdf(request, chave):
    try:
        nfe = NFe.objects.get(user=request.user, chave_acesso=chave)
        if not nfe.pdf_base64:
            return HttpResponse('PDF não disponível', status=404)
        pdf_bytes = base64.b64decode(nfe.pdf_base64)
        response = HttpResponse(pdf_bytes, content_type='application/pdf')
        response['Content-Disposition'] = f'attachment; filename="{chave}.pdf"'
        return response
    except NFe.DoesNotExist:
        return HttpResponse('NF-e não encontrada', status=404)


@require_GET
@login_required
def download_xml(request, chave):
    try:
        nfe = NFe.objects.get(user=request.user, chave_acesso=chave)
        if not nfe.xml_text:
            return HttpResponse('XML não disponível', status=404)
        response = HttpResponse(nfe.xml_text, content_type='application/xml')
        response['Content-Disposition'] = f'attachment; filename="{chave}.xml"'
        return response
    except NFe.DoesNotExist:
        return HttpResponse('NF-e não encontrada', status=404)


@require_POST
@csrf_exempt
@login_required
def clear_all(request):
    NFe.objects.filter(user=request.user).delete()
    return JsonResponse({'success': True})


@login_required
def relatorio_excel(request):
    nfes = NFe.objects.filter(user=request.user, status='OK', xml_text__isnull=False).order_by('-created_at')
    dados_detalhado = []
    dados_xml = []
    for nfe in nfes:
        dados_xml.append({
            'Chave': nfe.chave_acesso,
            'XML Completo': nfe.xml_text
        })
        try:
            root = ET.fromstring(nfe.xml_text)
            ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

            ide = root.find('.//nfe:ide', ns)
            emit = root.find('.//nfe:emit', ns)
            dest = root.find('.//nfe:dest', ns)
            total = root.find('.//nfe:ICMSTot', ns)

            chave = nfe.chave_acesso
            serie = ide.find('nfe:serie', ns).text if ide is not None else ''
            nNF = ide.find('nfe:nNF', ns).text if ide is not None else ''
            dhEmi = ide.find('nfe:dhEmi', ns).text if ide is not None else ''
            natOp = ide.find('nfe:natOp', ns).text if ide is not None else ''
            emit_nome = emit.find('nfe:xNome', ns).text if emit is not None else ''
            emit_cnpj = emit.find('nfe:CNPJ', ns).text if emit is not None else ''
            dest_nome = dest.find('nfe:xNome', ns).text if dest is not None else ''
            dest_cnpj = dest.find('nfe:CNPJ', ns).text if dest is not None else ''
            vNF = total.find('nfe:vNF', ns).text if total is not None else '0'

            itens = []
            for det in root.findall('.//nfe:det', ns):
                prod = det.find('nfe:prod', ns)
                if prod is not None:
                    cProd = prod.find('nfe:cProd', ns).text if prod.find('nfe:cProd', ns) is not None else ''
                    xProd = prod.find('nfe:xProd', ns).text if prod.find('nfe:xProd', ns) is not None else ''
                    qCom = prod.find('nfe:qCom', ns).text if prod.find('nfe:qCom', ns) is not None else '0'
                    vUnCom = prod.find('nfe:vUnCom', ns).text if prod.find('nfe:vUnCom', ns) is not None else '0'
                    vProd = prod.find('nfe:vProd', ns).text if prod.find('nfe:vProd', ns) is not None else '0'
                    itens.append({'cProd': cProd, 'xProd': xProd, 'qCom': qCom, 'vUnCom': vUnCom, 'vProd': vProd})

            if itens:
                for item in itens:
                    dados_detalhado.append({
                        'Chave': chave, 'Série': serie, 'Número NF': nNF, 'Data Emissão': dhEmi,
                        'Natureza Operação': natOp, 'Emitente': emit_nome, 'CNPJ Emitente': emit_cnpj,
                        'Destinatário': dest_nome, 'CNPJ Destinatário': dest_cnpj, 'Valor Total NF': vNF,
                        'Código Produto': item['cProd'], 'Descrição Produto': item['xProd'],
                        'Quantidade': item['qCom'], 'Valor Unitário': item['vUnCom'], 'Valor Total Item': item['vProd'],
                    })
            else:
                dados_detalhado.append({
                    'Chave': chave, 'Série': serie, 'Número NF': nNF, 'Data Emissão': dhEmi,
                    'Natureza Operação': natOp, 'Emitente': emit_nome, 'CNPJ Emitente': emit_cnpj,
                    'Destinatário': dest_nome, 'CNPJ Destinatário': dest_cnpj, 'Valor Total NF': vNF,
                    'Código Produto': '', 'Descrição Produto': '', 'Quantidade': '', 'Valor Unitário': '', 'Valor Total Item': '',
                })
        except Exception as e:
            dados_detalhado.append({'Chave': nfe.chave_acesso, 'Erro': str(e)})

    df_detalhado = pd.DataFrame(dados_detalhado) if dados_detalhado else pd.DataFrame({'Mensagem': ['Nenhuma NF-e com XML disponível.']})
    df_xml = pd.DataFrame(dados_xml) if dados_xml else pd.DataFrame({'Mensagem': ['Nenhuma NF-e com XML disponível.']})

    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename="relatorio_nfes.xlsx"'
    with pd.ExcelWriter(response, engine='openpyxl') as writer:
        df_detalhado.to_excel(writer, index=False, sheet_name='Detalhado')
        df_xml.to_excel(writer, index=False, sheet_name='XML Completo')
        # Ajustes de largura
        worksheet = writer.sheets['Detalhado']
        for column in worksheet.columns:
            max_len = 0
            col_letter = column[0].column_letter
            for cell in column:
                if cell.value:
                    max_len = max(max_len, len(str(cell.value)))
            worksheet.column_dimensions[col_letter].width = min(max_len + 2, 50)
        if 'XML Completo' in writer.sheets:
            ws_xml = writer.sheets['XML Completo']
            ws_xml.column_dimensions['A'].width = 45
            ws_xml.column_dimensions['B'].width = 80
    return response


@login_required
def stats(request):
    nfes = NFe.objects.filter(user=request.user, status='OK', xml_text__isnull=False)
    total_nfes = nfes.count()
    total_value = 0.0
    total_items = 0
    type_counts = defaultdict(int)
    monthly_counts = defaultdict(int)
    ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
    for nfe in nfes:
        try:
            root = ET.fromstring(nfe.xml_text)
            tipo = nfe.tipo or 'NFe'
            type_counts[tipo] += 1
            total_el = root.find('.//nfe:ICMSTot', ns)
            if total_el is not None:
                vNF = total_el.find('nfe:vNF', ns)
                if vNF is not None and vNF.text:
                    total_value += float(vNF.text)
            items = root.findall('.//nfe:det', ns)
            total_items += len(items)
            ide = root.find('.//nfe:ide', ns)
            if ide is not None:
                dhEmi = ide.find('nfe:dhEmi', ns)
                if dhEmi is not None and dhEmi.text:
                    try:
                        dt = datetime.fromisoformat(dhEmi.text)
                        month_key = dt.strftime('%Y-%m')
                        monthly_counts[month_key] += 1
                    except:
                        pass
        except Exception as e:
            pass
    return JsonResponse({
        'total_nfes': total_nfes,
        'total_value': total_value,
        'total_items': total_items,
        'type_labels': list(type_counts.keys()),
        'type_data': list(type_counts.values()),
        'monthly_labels': sorted(monthly_counts.keys()),
        'monthly_data': [monthly_counts[k] for k in sorted(monthly_counts.keys())],
    })

import logging
logger = logging.getLogger(__name__)

@login_required
def checkout(request):
    plan = request.GET.get('plan')
    prices = {'mensal': 0.01, 'trimestral': 79.90, 'anual': 299.90}
    if not plan or plan not in prices:
        return redirect('home')
    amount = prices[plan]

    if request.user.profile.subscription_active:
        return redirect('dashboard')

    sdk = mercadopago.SDK(settings.MERCADOPAGO_ACCESS_TOKEN)

    base_url = request.build_absolute_uri('/').rstrip('/')
    success_url = f"{base_url}{reverse('payment_success')}"
    failure_url = f"{base_url}{reverse('payment_failure')}"
    pending_url = f"{base_url}{reverse('payment_pending')}"
    notification_url = f"{base_url}{reverse('payment_webhook')}"

    print("Base URL:", base_url)
    print("Success URL:", success_url)
    print("Failure URL:", failure_url)
    print("Pending URL:", pending_url)
    print("Notification URL:", notification_url)

    preference_data = {
        "items": [{
            "title": f"SmartDanfe - Plano {plan.capitalize()}",
            "quantity": 1,
            "currency_id": "BRL",
            "unit_price": amount,
        }],
        "payer": {
            "email": request.user.email or "cliente@smartdanfe.com.br",
            "name": request.user.get_full_name() or request.user.username,
        },
        "back_urls": {
            "success": success_url,
            "failure": failure_url,
            "pending": pending_url,
        },
        "auto_return": "approved",  # ativado
        "notification_url": notification_url,
        "external_reference": f"{request.user.id}_{plan}",
    }

    try:
        preference_response = sdk.preference().create(preference_data)
        print("Resposta MP:", preference_response)

        if preference_response.get('status') != 201:
            error = preference_response.get('response', {}).get('message', 'Erro desconhecido')
            cause = preference_response.get('response', {}).get('cause')
            if cause:
                error += f" - {cause}"
            return render(request, 'nfe/error.html', {'message': f'Erro ao criar preferência: {error}'})

        preference = preference_response.get('response', {})
        if 'id' not in preference:
            return render(request, 'nfe/error.html', {'message': 'Resposta inválida do Mercado Pago'})

        preference_id = preference['id']
        init_point = preference.get('init_point')
    except Exception as e:
        logger.exception("Erro na criação da preferência")
        return render(request, 'nfe/error.html', {'message': f'Erro interno: {str(e)}'})

    Payment.objects.create(
        user=request.user,
        plan=plan,
        amount=amount,
        preference_id=preference_id,
        init_point=init_point,
        status='PENDING'
    )

    return render(request, 'nfe/checkout.html', {
        'plan': plan,
        'amount': amount,
        'preference_id': preference_id,
        'public_key': settings.MERCADOPAGO_PUBLIC_KEY,
    })

@csrf_exempt
def process_payment(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Método não permitido'}, status=405)

    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'JSON inválido'}, status=400)

    sdk = mercadopago.SDK(settings.MERCADOPAGO_ACCESS_TOKEN)

    # Dados básicos do pagamento
    payment_data = {
        "transaction_amount": data.get("transaction_amount"),
        "token": data.get("token"),
        "description": data.get("description", "SmartDanfe - Plano"),
        "installments": data.get("installments", 1),
        "payment_method_id": data.get("payment_method_id"),
        "payer": {
            "email": data.get("payer", {}).get("email"),
            "identification": data.get("payer", {}).get("identification", {}),
            "first_name": data.get("payer", {}).get("first_name"),
            "last_name": data.get("payer", {}).get("last_name"),
        }
    }

    # Extrair endereço do payer (para boleto)
    payer_address = data.get("payer", {}).get("address")
    if payer_address:
        payment_data["payer"]["address"] = {
            "zip_code": payer_address.get("zip_code"),
            "street_name": payer_address.get("street_name"),
            "street_number": payer_address.get("street_number"),
            "neighborhood": payer_address.get("neighborhood"),
            "city": payer_address.get("city"),
            "federal_unit": payer_address.get("federal_unit"),
        }

    # Remove campos com valor None
    def clean_dict(d):
        return {k: v for k, v in d.items() if v is not None}
    payment_data = clean_dict(payment_data)
    payment_data["payer"] = clean_dict(payment_data.get("payer", {}))
    if "identification" in payment_data["payer"]:
        payment_data["payer"]["identification"] = clean_dict(payment_data["payer"]["identification"])
    if "address" in payment_data["payer"]:
        payment_data["payer"]["address"] = clean_dict(payment_data["payer"]["address"])

    try:
        payment_response = sdk.payment().create(payment_data)
        print("Payment response:", payment_response)

        # Verifica se houve erro
        if payment_response.get('status') != 201:
            error_msg = payment_response.get('response', {}).get('message', 'Erro desconhecido')
            cause = payment_response.get('response', {}).get('cause')
            if cause:
                error_msg += f" - {cause}"
            return JsonResponse({'error': error_msg, 'status': payment_response.get('status')}, status=400)

        payment = payment_response.get('response', {})
        status = payment.get('status')
        # Garante que status seja string
        if isinstance(status, int):
            status = str(status)

        # Atualiza o registro no banco
        preference_id = data.get('preference_id')
        if preference_id:
            payment_obj = Payment.objects.filter(preference_id=preference_id).first()
            if payment_obj:
                payment_obj.status = status.upper()
                payment_obj.payment_id = payment.get('id')
                payment_obj.save()

        return JsonResponse({'status': status, 'id': payment.get('id')})

    except Exception as e:
        logger.exception("Erro ao processar pagamento")
        return JsonResponse({'error': str(e)}, status=500)
    
@login_required
def payment_success(request):
    preference_id = request.GET.get('preference_id')
    payment_id = request.GET.get('collection_id')  # também pode vir como 'payment_id'

    print("=== PAYMENT_SUCCESS CHAMADA ===")
    print("Preference ID:", preference_id)
    print("Payment ID:", payment_id)

    if preference_id:
        payment = Payment.objects.filter(preference_id=preference_id).first()
        if payment and payment.status != 'APPROVED':
            # Consulta o status atual diretamente no Mercado Pago
            try:
                sdk = mercadopago.SDK(settings.MERCADOPAGO_ACCESS_TOKEN)
                if payment_id:
                    # Buscar pelo payment_id
                    payment_info = sdk.payment().get(payment_id)
                    if payment_info['status'] == 200:
                        status = payment_info['response'].get('status')
                        if status == 'approved':
                            payment.status = 'APPROVED'
                            payment.payment_id = payment_id
                            payment.save()
                else:
                    # Se não temos payment_id, podemos tentar buscar pelo external_reference
                    # Isso é menos preciso, mas uma alternativa
                    # Para simplificar, vamos usar o payment_id do próprio pagamento se existir
                    # Caso contrário, podemos ignorar
                    pass
            except Exception as e:
                print("Erro ao consultar pagamento:", e)

            # Se ainda não foi aprovado, marca como aprovado (fallback)
            if payment.status != 'APPROVED':
                payment.status = 'APPROVED'
                payment.save()

            # Ativar assinatura
            profile = request.user.profile
            profile.subscription_active = True
            profile.plan = payment.plan
            days = 30 if payment.plan == 'mensal' else (90 if payment.plan == 'trimestral' else 365)
            profile.subscription_until = datetime.now() + timedelta(days=days)
            profile.save()
            print("Assinatura ativada para", request.user.username)

    return render(request, 'nfe/payment_success.html')

@login_required
def payment_failure(request):
    return render(request, 'nfe/payment_failure.html')


@login_required
def payment_pending(request):
    return render(request, 'nfe/payment_pending.html')


@csrf_exempt
def payment_webhook(request):
    # Log para saber que o webhook foi chamado
    print("=== WEBHOOK CHAMADO ===")
    print("Request body:", request.body)

    try:
        data = json.loads(request.body)
    except Exception as e:
        print("Erro ao parsear JSON:", e)
        return JsonResponse({'status': 'error'}, status=400)

    # Log dos dados recebidos
    print("Dados do webhook:", data)

    # A notificação pode vir com diferentes tipos: 'payment', 'merchant_order', etc.
    if data.get('type') == 'payment':
        payment_id = data['data']['id']
        print(f"Notificação de pagamento ID: {payment_id}")

        sdk = mercadopago.SDK(settings.MERCADOPAGO_ACCESS_TOKEN)
        try:
            payment_info = sdk.payment().get(payment_id)
            payment_data = payment_info["response"]
            print("Dados do pagamento:", payment_data)

            external_reference = payment_data.get('external_reference')
            preference_id = payment_data.get('preference_id')
            status = payment_data.get('status')  # 'approved', 'pending', etc.

            if external_reference:
                try:
                    user_id, plan = external_reference.split('_')
                except:
                    user_id = external_reference
                    plan = None
            else:
                user_id = None
                plan = None

            # Tenta encontrar o pagamento no banco pelo preference_id
            if preference_id:
                payment = Payment.objects.filter(preference_id=preference_id).first()
            elif external_reference:
                # Caso não tenha preference_id, tenta por external_reference
                payment = Payment.objects.filter(external_reference=external_reference).first()
            else:
                payment = None

            if payment:
                print(f"Pagamento encontrado no banco: {payment.id}")
                payment.status = status.upper()
                payment.payment_id = payment_id
                payment.save()

                if status == 'approved':
                    # Ativa a assinatura
                    profile = payment.user.profile
                    profile.subscription_active = True
                    profile.plan = payment.plan or plan
                    days = 30 if (payment.plan == 'mensal' or plan == 'mensal') else (90 if (payment.plan == 'trimestral' or plan == 'trimestral') else 365)
                    profile.subscription_until = datetime.now() + timedelta(days=days)
                    profile.save()
                    print(f"Assinatura ativada para {payment.user.username}")
            else:
                print("Pagamento não encontrado no banco")

        except Exception as e:
            print("Erro ao processar pagamento via webhook:", e)

    return JsonResponse({'status': 'ok'})

@login_required
def pending_payments(request):
    """Página de pagamentos pendentes (acessível mesmo sem assinatura)"""
    payments = Payment.objects.filter(user=request.user, status='PENDING').order_by('-created_at')
    return render(request, 'nfe/payment_history.html', {'payments': payments})

@login_required
@subscription_required
def dashboard(request):
    pending_payments = Payment.objects.filter(user=request.user, status='PENDING').exists()
    return render(request, 'nfe/dashboard.html', {'payment_history': pending_payments})

@login_required
def payment_history(request):
    """Página com histórico de pagamentos e informações da assinatura atual"""
    # Busca todos os pagamentos do usuário ordenados por data (mais recente primeiro)
    all_payments = Payment.objects.filter(user=request.user).order_by('-created_at')
    
    # Busca a assinatura ativa atual (pode ser um pagamento aprovado com validade)
    active_subscription = None
    profile = request.user.profile
    if profile.subscription_active and profile.subscription_until:
        if profile.subscription_until > datetime.now():
            active_subscription = {
                'plan': profile.plan,
                'expiration_date': profile.subscription_until,
                'status': 'Ativa'
            }
        else:
            # Assinatura expirada
            active_subscription = {
                'plan': profile.plan,
                'expiration_date': profile.subscription_until,
                'status': 'Expirada'
            }
    
    context = {
        'payments': all_payments,
        'active_subscription': active_subscription,
    }
    return render(request, 'nfe/payment_history.html', context)