import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import functions_framework

SENDER = "hmy@bu.edu"  
RECIPIENTS = ["hmy@bu.edu", "ebaykurt@bu.edu", "zehui@bu.edu", "sanjal@bu.edu"]

@functions_framework.http
def send_notification(request):
    """Cloud Function to send email via SendGrid"""
    try:
        message = Mail(
            from_email=SENDER,
            to_emails=RECIPIENTS,
            subject="✅ GitHub Data Pipeline Completed Successfully",
            html_content="""
            <h3>🎉 Pipeline Execution Successful</h3>
            <p><b>DAG:</b> github_data_pipeline_v3</p>
            <p><b>Output:</b> ba-882-fall25-team8.ml_results.repo_cluster_summary</p>
            <p>All tasks completed successfully. Check BigQuery for results.</p>
            """
        )

        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        sg.send(message)

        return {"status": "Email sent successfully"}
    except Exception as e:
        return {"error": str(e)}
