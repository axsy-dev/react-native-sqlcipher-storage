type PreloadInit = {
  preload: {
    init: () => void;
  };
};

declare const db: PreloadInit;
