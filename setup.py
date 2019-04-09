import setuptools


setuptools.setup(
    name='data-tools',
    version='0.0.1',

    author='Max Zheng',
    author_email='mzheng@confluent.io',

    description='Data transformation tools',
    long_description=open('README.md').read(),

    url='https://github.com/confluentinc/data-tools',

    install_requires=open('requirements.txt').read(),

    license='MIT',

    packages=setuptools.find_packages(),
    include_package_data=True,

    python_requires='>=3.7',
    setup_requires=['setuptools-git', 'wheel'],

    entry_points={
       'console_scripts': [
           'transform = confluent.data.scripts:transform',
       ],
    },

    # Standard classifiers at https://pypi.org/classifiers/
    classifiers=[
      'Development Status :: 5 - Production/Stable',

      'Intended Audience :: Science/Research',
      'Topic :: Text Processing',

      'License :: OSI Approved :: MIT License',

      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.7',
    ],

    keywords='data transformation',
)
