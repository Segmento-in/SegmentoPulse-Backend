"""
Brevo (Sendinblue) Email Service
Handles email subscriptions, newsletters, and unsubscribe functionality
"""
import hashlib
import secrets
from typing import List, Dict, Optional
from datetime import datetime

import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException

from app.config import settings


class BrevoEmailService:
    """Email service using Brevo API"""
    
    def __init__(self):
        # Configure API key authorization
        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = settings.BREVO_API_KEY
        
        self.api_instance = sib_api_v3_sdk.TransactionalEmailsApi(
            sib_api_v3_sdk.ApiClient(configuration)
        )
        self.contacts_api = sib_api_v3_sdk.ContactsApi(
            sib_api_v3_sdk.ApiClient(configuration)
        )
        self.account_api = sib_api_v3_sdk.AccountApi(
            sib_api_v3_sdk.ApiClient(configuration)
        )
    
    def get_account_info(self) -> Optional[Dict]:
        """
        Get Brevo account information including email credits
        
        Returns: {
            'email_credits': int,  # Remaining email credits
            'plan_type': str,
            'credits_type': str  # 'monthly' or 'payAsYouGo'
        }
        """
        try:
            account = self.account_api.get_account()
            
            # Extract email plan info
            email_plan = account.plan[0] if account.plan else None
            
            if not email_plan:
                print("⚠️  No email plan found in Brevo account")
                return None
            
            return {
                'email_credits': email_plan.credits,
                'plan_type': email_plan.type,
                'credits_type': email_plan.credits_type
            }
        except ApiException as e:
            print(f"Brevo API error getting account info: {e}")
            return None
        except Exception as e:
            print(f"Error getting account info: {e}")
            return None
    
    def check_quota(self, required_emails: int) -> Dict[str, any]:
        """
        Check if there are enough email credits for the send job
        
        Args:
            required_emails: Number of emails we want to send
        
        Returns: {
            'sufficient': bool,
            'remaining_credits': int,
            'required': int,
            'shortfall': int  # How many we can't send (0 if sufficient)
        }
        """
        account_info = self.get_account_info()
        
        if not account_info:
            # If we can't check quota, assume unlimited (best effort)
            print("⚠️  Could not check Brevo quota, proceeding with send")
            return {
                'sufficient': True,
                'remaining_credits': -1,  # Unknown
                'required': required_emails,
                'shortfall': 0
            }
        
        remaining = account_info['email_credits']
        sufficient = remaining >= required_emails
        shortfall = max(0, required_emails - remaining)
        
        return {
            'sufficient': sufficient,
            'remaining_credits': remaining,
            'required': required_emails,
            'shortfall': shortfall,
            'plan_type': account_info.get('plan_type', 'unknown')
        }
    
    def generate_unsubscribe_token(self, email: str) -> str:
        """Generate unique token for unsubscribe links"""
        # Use email + timestamp + random salt for uniqueness
        salt = secrets.token_urlsafe(16)
        data = f"{email}:{datetime.now().isoformat()}:{salt}"
        return hashlib.sha256(data.encode()).hexdigest()
    
    def generate_unsubscribe_link(self, token: str) -> str:
        """Generate unsubscribe URL"""
        base_url = settings.FRONTEND_URL or "https://segmento.in"
        return f"{base_url}/api/unsubscribe?token={token}"
    
    def send_welcome_email(self, email: str, name: str, token: str) -> bool:
        """Send welcome email to new subscriber"""
        try:
            unsubscribe_link = self.generate_unsubscribe_link(token)
            
            # Create email object
            send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
                to=[{"email": email, "name": name}],
                sender={
                    "email": settings.BREVO_SENDER_EMAIL,
                    "name": settings.BREVO_SENDER_NAME
                },
                subject="Welcome to SegmentoPulse! 🚀",
                html_content=f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="UTF-8">
                    <style>
                        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; }}
                        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }}
                        .content {{ padding: 30px; background: #f9f9f9; }}
                        .button {{ background: #667eea; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block; margin: 20px 0; }}
                        .footer {{ padding: 20px; text-align: center; font-size: 12px; color: #666; background: #f0f0f0; }}
                        .benefits {{ background: white; padding: 20px; border-radius: 8px; margin: 20px 0; }}
                        .benefits li {{ margin: 10px 0; }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>Welcome to SegmentoPulse!</h1>
                    </div>
                    <div class="content">
                        <h2>Hi {name},</h2>
                        <p>Thanks for subscribing to SegmentoPulse! We're excited to have you on board.</p>
                        
                        <div class="benefits">
                            <p><strong>You'll receive:</strong></p>
                            <ul>
                                <li>✅ Weekly curated tech news digests</li>
                                <li>✅ Data security & privacy updates</li>
                                <li>✅ Cloud computing insights</li>
                                <li>✅ AI & machine learning trends</li>
                            </ul>
                        </div>
                        
                        <p>Stay ahead of the curve with the latest technology intelligence!</p>
                        
                        <a href="https://segmento.in/pulse" class="button">Explore Latest News →</a>
                    </div>
                    <div class="footer">
                        <p>You're receiving this because you subscribed to SegmentoPulse.</p>
                        <p><a href="{unsubscribe_link}" style="color: #667eea;">Unsubscribe</a> | <a href="https://segmento.in" style="color: #667eea;">Visit Website</a></p>
                        <p>© 2026 Segmento. All rights reserved.</p>
                    </div>
                </body>
                </html>
                """,
                text_content=f"""
                Welcome to SegmentoPulse!
                
                Hi {name},
                
                Thanks for subscribing! You'll receive:
                - Weekly tech news digests
                - Data security updates
                - Cloud computing insights
                - AI trends
                
                Unsubscribe: {unsubscribe_link}
                """
            )
            
            # Send email
            self.api_instance.send_transac_email(send_smtp_email)
            return True
            
        except ApiException as e:
            print(f"Brevo API error sending welcome email: {e}")
            return False
        except Exception as e:
            print(f"Error sending welcome email: {e}")
            return False
    
    def send_newsletter(
        self, 
        preference: str,
        subject: str,
        greeting: str,
        articles: List[Dict],
        subscribers: List[Dict],
        max_send: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Send newsletter to subscribers with QUOTA-AWARE sending
        
        Args:
            preference: Newsletter preference (Morning/Afternoon/Evening/Weekly/Monthly)
            subject: Email subject line
            greeting: Personalized greeting text
            articles: List of article dictionaries
            subscribers: List of subscriber dictionaries
            max_send: Optional limit on number of emails (for quota management)
            
        Returns: {
            "sent": count, 
            "failed": count,
            "quota_limited": bool,  # True if we hit quota limits
            "remaining_credits": int  # Brevo credits remaining after send
        }
        """
        sent = 0
        failed = 0
        quota_limited = False
        
        # QUOTA CHECK: Determine how many we can actually send
        total_subscribers = len(subscribers)
        quota_status = self.check_quota(total_subscribers)
        
        if not quota_status['sufficient']:
            print(f"")
            print(f"{'='*80}")
            print(f"⚠️  QUOTA WARNING: Brevo API Limit Reached!")
            print(f"   Requested: {quota_status['required']} emails")
            print(f"   Available: {quota_status['remaining_credits']} credits")
            print(f"   Shortfall: {quota_status['shortfall']} emails WILL NOT be sent")
            print(f"   Plan: {quota_status.get('plan_type', 'unknown')}")
            print(f"{'='*80}")
            print(f"")
            quota_limited = True
            # Limit sending to available quota
            max_send = quota_status['remaining_credits']
        
        # Apply quota limit if set
        subscribers_to_send = subscribers[:max_send] if max_send else subscribers
        
        print(f"📧 Sending to {len(subscribers_to_send)} of {total_subscribers} subscribers")
        if quota_limited:
            print(f"   ⚠️  {total_subscribers - len(subscribers_to_send)} subscribers SKIPPED due to quota")
        
        for subscriber in subscribers_to_send:
            if not subscriber.get('subscribed', True):
                continue
                
            try:
                email = subscriber['email']
                name = subscriber.get('name', 'Subscriber')
                token = subscriber.get('token', '')
                
                unsubscribe_link = self.generate_unsubscribe_link(token)
                
                # Build articles HTML
                articles_html = ""
                for article in articles[:10]:  # Top 10 articles
                    articles_html += f"""
                    <div style="margin: 20px 0; padding: 15px; background: white; border-radius: 8px;">
                        <h3 style="margin: 0 0 10px 0;">
                            <a href="{article.get('url', '#')}" style="color: #667eea; text-decoration: none;">
                                {article.get('title', 'Article')}
                            </a>
                        </h3>
                        <p style="color: #666; margin: 0;">{article.get('description', '')[:200]}...</p>
                        <p style="margin: 10px 0 0 0; font-size: 12px; color: #999;">
                            {article.get('source', 'Unknown')} • {article.get('publishedAt', '')}
                        </p>
                    </div>
                    """
                
                send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
                    to=[{"email": email, "name": name}],
                    sender={
                        "email": settings.BREVO_SENDER_EMAIL,
                        "name": settings.BREVO_SENDER_NAME
                    },
                    subject=subject,
                    html_content=f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <meta charset="UTF-8">
                        <style>
                            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; background: #f9f9f9; }}
                            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center; }}
                            .content {{ padding: 30px; }}
                            .footer {{ padding: 20px; text-align: center; font-size: 12px; color: #666; background: #f0f0f0; }}
                        </style>
                    </head>
                    <body>
                        <div class="header">
                            <h1>{preference} Newsletter</h1>
                        </div>
                        <div class="content">
                            <h2>Hi {name},</h2>
                            <p>{greeting}</p>
                            {articles_html}
                            <p style="text-align: center; margin-top: 30px;">
                                <a href="https://segmento.in/pulse" style="background: #667eea; color: white; padding: 12px 30px; text-decoration: none; border-radius: 5px; display: inline-block;">
                                    Read More on SegmentoPulse →
                                </a>
                            </p>
                        </div>
                        <div class="footer">
                            <p><a href="{unsubscribe_link}" style="color: #667eea;">Unsubscribe</a> | <a href="https://segmento.in" style="color: #667eea;">Visit Website</a></p>
                            <p>© 2026 Segmento. All rights reserved.</p>
                        </div>
                    </body>
                    </html>
                    """
                )
                
                self.api_instance.send_transac_email(send_smtp_email)
                sent += 1
                
            except Exception as e:
                print(f"Failed to send to {subscriber.get('email')}: {e}")
                failed += 1
        
        # Get final quota status after sending
        final_quota = self.check_quota(0)  # Just to get remaining credits
        
        return {
            "sent": sent, 
            "failed": failed,
            "quota_limited": quota_limited,
            "remaining_credits": final_quota.get('remaining_credits', -1),
            "skipped_count": total_subscribers - len(subscribers_to_send) if quota_limited else 0
        }
    
    def send_unsubscribe_confirmation(self, email: str, name: str) -> bool:
        """Send confirmation email after unsubscribe"""
        try:
            send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
                to=[{"email": email, "name": name}],
                sender={
                    "email": settings.BREVO_SENDER_EMAIL,
                    "name": settings.BREVO_SENDER_NAME
                },
                subject="You've been unsubscribed from SegmentoPulse",
                html_content=f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="UTF-8">
                    <style>
                        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; }}
                        .content {{ padding: 40px; text-align: center; }}
                        .footer {{ padding: 20px; text-align: center; font-size: 12px; color: #666; background: #f0f0f0; }}
                    </style>
                </head>
                <body>
                    <div class="content">
                        <h1>You've been unsubscribed</h1>
                        <p>Hi {name},</p>
                        <p>You've successfully unsubscribed from SegmentoPulse newsletters.</p>
                        <p>We're sorry to see you go! You won't receive any more emails from us.</p>
                        <p style="margin-top: 30px;">
                            <a href="https://segmento.in/pulse" style="color: #667eea;">Changed your mind? Resubscribe →</a>
                        </p>
                    </div>
                    <div class="footer">
                        <p>© 2026 Segmento. All rights reserved.</p>
                    </div>
                </body>
                </html>
                """
            )
            
            self.api_instance.send_transac_email(send_smtp_email)
            return True
            
        except Exception as e:
            print(f"Error sending unsubscribe confirmation: {e}")
            return False


# Singleton instance
_brevo_service = None

def get_brevo_service() -> BrevoEmailService:
    """Get singleton Brevo email service instance"""
    global _brevo_service
    if _brevo_service is None:
        _brevo_service = BrevoEmailService()
    return _brevo_service
