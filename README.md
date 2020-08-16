# Recommandation System
Recommendation system using python on Amazon customer review data with collaborative filtering in MapReduce with AWS EMR.
# To run on 20 EMR nodes:
!python AmazonSimilarityReview.py -r emr --num-ec2-instances=20 

Troubleshooting EMR jobs (subsitute your job ID):

!python -m mrjob.tools.emr.fetch_logs --find-failure j-1NXMMBNEQHAFT
