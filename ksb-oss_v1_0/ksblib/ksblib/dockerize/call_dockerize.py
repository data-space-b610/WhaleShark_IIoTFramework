from ksblib.dockerize import Dockerize


# Bridging script between KSB framework and ksblib.dockerize
if __name__ == '__main__':
    import sys
    import argparse
    
    # Get command line arguments. For instance:
    # Local file:
    # python ksblib/dockerize/call_dockerize.py chitchat_image /Users/kim/temp/chitchat_preprocess --python_libraries nltk:scipy==1.0 --port 8080 --user_args http://test:9090/predict

    # HDFS:
    # python ksblib/dockerize/call_dockerize.py chitchat_image hdfs://csle1:9000/user/ksbuser_etri_re_kr/model/chatbot/python_code/chitchat --python_libraries nltk:numpy==1.15.0:scipy==1.0 --port 8080 --user_args=http://test:9090/predict

    # Parse command line arguments.
    parser = argparse.ArgumentParser(
        description='Call KSBLib.dockerize library ' +
                    'to build a custom docker image.')
    parser.add_argument('image_name', type=str, help='docker image name')
    parser.add_argument('src_folder', type=str,
                        help='folder path of source files')
    parser.add_argument('--python_libraries', type=str, nargs='?',
                        help='python libraries, separated by ":", e.g. ' +
                             'numpy:scipy')
    parser.add_argument('--port', nargs='?', default=8080, type=int,
                            help='server port')
    parser.add_argument('--user_args', nargs='?', type=str,
                        help='This parameter will be picklized and saved ' +
                             'as "ksb_user_args.pkl" in the "modules" folder.')
    args = parser.parse_args(sys.argv[1:])

    # Get Python library list.
    if args.python_libraries is not None:
        libraries = args.python_libraries.split(':')
    else:
        libraries = None

    # Build a custom Docker image.
    drize = Dockerize(args.image_name, args.src_folder,
                      requirements=libraries, user_args=args.user_args)
    drize.build()

    # Run the build image.
    drize.run(args.port)
