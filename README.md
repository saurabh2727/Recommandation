# Recommandation System
Recommendation system using python on Amazon customer review data in MapReduce with AWS EMR.
To run on 20 EMR nodes:
!python MovieSimilaritiesLarge.py -r emr --num-ec2-instances=20 --items=ml-1m/movies.dat ml-1m/ratings.dat

Troubleshooting EMR jobs (subsitute your job ID):
!python -m mrjob.tools.emr.fetch_logs --find-failure j-1NXMMBNEQHAFT
