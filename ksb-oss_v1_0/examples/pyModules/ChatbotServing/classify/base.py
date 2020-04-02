import sys


def main_func(x):

    # get input
    input = x['input']
    text = tune_text(input)

    # call your function
    out = classify(text)

    return {'output': str(out)}


def tune_text(sentence):
    x = sentence.lower()
    x = x.replace('?', ' ?')
    x = x.replace('!', ' !')
    x = x.replace('.', ' .')
    x = x.replace(',', ' ,')
    x = x.replace('  ', ' ')
    x = x.replace("'", "")
    userinput = x.split(' ')

    return userinput


def classify(input):
    if 'go' in input:
        out = 1 # Travel agency
    else:
        out = 0 # Chitchat

    return out



def getInput():
    print('>>')
    sentence = sys.stdin.readline()
    x = sentence.lower()
    x = x.replace('?', ' ?')
    x = x.replace('!', ' !')
    x = x.replace('.', ' .')
    x = x.replace(',', ' ,')
    x = x.replace('  ', ' ')
    x = x.replace("'", "")
    userinput = x.split(' ')
    return userinput

if __name__ == '__main__':
    input = getInput()
    print(input)
    classify(input)


