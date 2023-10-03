# cwl_platform

This repo is intended to be a git submodule for other launchers.

To ADD in your own launcher
```
cd launcher/src
git submodule add https://biogit.pri.bms.com/NGS/cwl_platform
```

If updating the submodule in a remote project (launcher), do the following to push the submodule changes, commit the changes on a new branch, and push the branch and open a PR

If cwl_platform is already a submodule, then execute
```
git submodule init
git submodule update
```

