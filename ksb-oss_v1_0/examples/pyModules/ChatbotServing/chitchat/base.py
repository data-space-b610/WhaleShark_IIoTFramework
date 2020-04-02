import sys


def main_func(x):

    # get input
    input = x['input']
    text = tune_text(input)

    # call your function
    output_text = predict(text)

    return {'output': str(output_text)}


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


def predict(x):

    text = 'Hi, I am chitchat'

    return text


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
    # print(input)
    out = predict(input)
    print(out)
